//! Transaction implementation and cache coordination.
//!
//! Cache paths use `Entry::Any(val.clone())` for concrete `Arc<T>` values that must coerce to
//! `Arc<dyn Any + Send + Sync>`; `Arc::clone(&val)` does not perform that unsized coercion.
#![allow(clippy::clone_on_ref_ptr)]
// `Transaction`'s pub methods take `K: KVKey` / `K::Value: KVValue`.
// Both traits are `pub(crate)` (their `pub` declarations are gated by
// `pub(crate) use` re-exports in `kvs/mod.rs`), so the lint flags every
// such method. The visibility is intentional — silence at module scope.
#![allow(private_bounds, private_interfaces)]

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use surrealdb_cnf::CommonConfig;
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use tokio::sync::{Mutex, Notify};
use tokio::time::sleep;
use tracing::Instrument;
use uuid::Uuid;
use web_time::Instant;

use super::api::{
	Batch, KeyVisitor, KeysBatch, ScanChunkStats, ScanCursorKeys, ScanCursorVals, ValVisitor,
	ValsBatch,
};
use super::{TransactionFactory, TransactionType, Val, util};
use crate::catalog::providers::{
	ApiProvider, AuthorisationProvider, BoxProviderFut, BucketProvider, CatalogProvider,
	DatabaseProvider, NamespaceProvider, NodeProvider, RootProvider, TableProvider, UserProvider,
};
use crate::catalog::{
	self, ApiDefinition, ConfigDefinition, DatabaseDefinition, DatabaseId, DefaultConfig, IndexId,
	NamespaceDefinition, NamespaceId, Record, TableDefinition, TableId,
};
use crate::cf::Changefeed;
use crate::ctx::Context;
use crate::dbs::node::Node;
use crate::doc::CursorRecord;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::planner::ScanDirection;
use crate::key::database::all::DatabaseRoot;
use crate::key::database::sq::Sq;
use crate::key::index::all as index_all;
use crate::key::root::rc::{Expunge, ReclaimKind};
use crate::key::table::bg::BgMutationPrefix;
use crate::key::table::br::Br;
use crate::key::table::bs::Bs;
use crate::key::table::ix as table_ix;
use crate::key::{KVKey, KVKeyDecode, KVRange, KVValue, Key, KeyRange};
use crate::kvs::cache::tx::TransactionCache;
use crate::kvs::index::{
	BuildGeneration, BuildTicket, BuildTicketMutationSeq, IndexBuildPhase, IndexBuildReportStatus,
	IndexBuilder,
};
use crate::kvs::sequences::Sequences;
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::kvs::{
	Direction, Error as KvsError, IntoBytes, Transactor, cache, is_retryable_transaction_conflict,
};
use crate::lq::writer::LiveEventBuffer;
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
	/// The live-query event buffer (dedicated keyspace, Router engine).
	live_events: OnceLock<LiveEventBuffer>,
	/// Async event trigger
	async_event_trigger: Arc<Notify>,
	/// Do we have to trigger async events after the commit?
	trigger_async_event: AtomicBool,
	/// Write-cardinality guard: maximum number of individual key writes this
	/// transaction may buffer before further writes fail. Unset by default,
	/// leaving the transaction unbounded. Armed once — by the executor for
	/// its own statement transactions via [`Self::with_write_keys_limit`],
	/// or through [`Self::arm_write_keys_limit`] for externally-supplied
	/// (client-owned, `Arc`-wrapped) transactions when statements execute on
	/// them. `OnceLock` for the same reason as `tenant_identity`: the
	/// external path only sees the transaction after it is wrapped in an
	/// `Arc`.
	write_keys_limit: OnceLock<NonZeroU64>,
	/// Set when the write-cardinality guard trips. A poisoned transaction
	/// refuses COMMIT (rolling back instead), because the writes buffered
	/// before the failing reservation are a partial statement; CANCEL
	/// behaves as normal. This is what preserves the guard's atomic
	/// rollback contract on client-owned (RPC/SDK) transactions, whose
	/// lifecycle the executor does not manage.
	write_guard_poisoned: AtomicBool,
	/// Number of write slots reserved against the write-cardinality guard.
	/// Each write operation atomically reserves its slot *before* the
	/// storage call, so concurrent writes on the same transaction (e.g.
	/// graph-pointer maintenance joining several deletes) can never admit
	/// more writes than the limit through a stale read of the counter.
	/// Independent of [`TransactionMetrics`], which records successful
	/// operations for observability: a failed write keeps its reservation,
	/// and a range delete reserves one slot regardless of its (unreported)
	/// per-key expansion.
	guarded_writes: AtomicU64,
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

		let tx = self.tf.transaction(TransactionType::Write, self.sequences.clone()).await?;
		let ikb = IndexKeyBase::new(self.ns, self.db, self.tb.clone(), self.ix);
		let index_prefix = index_all::AllIndexRoot {
			prefix: crate::key::database::all::DatabaseRoot {
				ns: self.ns,
				db: self.db,
			},
			tb: Cow::Borrowed(&self.tb),
			ix: self.ix,
		}
		.encode_range()?;
		let result: Result<()> = async {
			tx.tr.del(ikb.new_bs_key().encode_key()?).await.map_err(Error::from)?;
			tx.tr.delr(ikb.new_bg_all_generations_range()?).await.map_err(Error::from)?;
			tx.tr.delr(ikb.new_bp_all_generations_range()?).await.map_err(Error::from)?;
			tx.tr.delr(ikb.new_br_all_generations_range()?).await.map_err(Error::from)?;
			tx.tr.delr(index_prefix).await.map_err(Error::from)?;
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
	key: Key<'static>,
	val: Val,
}

impl IndexBuildReservationRelease {
	pub(crate) fn new(
		tf: TransactionFactory,
		sequences: Sequences,
		node: Uuid,
		key: Key<'static>,
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
		let tx = self.tf.transaction(TransactionType::Write, self.sequences.clone()).await?;

		#[cfg(test)]
		if let Err(err) = maybe_inject_non_retryable_error(
			NonRetryableErrorSite::ConcurrentIndexReservationRelease,
			self.node,
		) {
			let _ = tx.tr.cancel().await;
			return Err(err);
		}

		match tx.tr.delc(self.key.as_borrowed(), Some(&self.val)).await {
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
		let range = BgMutationPrefix {
			prefix: br.prefix,
			tb: Cow::Borrowed(br.tb.as_ref()),
			ix: br.ix,
			generation: br.generation,
			ticket: br.ticket,
		}
		.encode_range()?;

		let bs = Bs {
			prefix: br.prefix,
			tb: Cow::Borrowed(br.tb.as_ref()),
			ix: br.ix,
		};

		let reason = format!(
			"Failed to release durable index-build reservation for generation {} ticket {} after transaction close: {release_err}",
			br.generation, br.ticket
		);

		loop {
			let tx = self.tf.transaction(TransactionType::Write, self.sequences.clone()).await?;

			let current_reservation = match tx.tr.get(self.key.as_borrowed(), None).await {
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

			match tx.tr.keys(range.as_borrowed(), 1, 0, None).await {
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

			let current = match tx.get_key(&bs, None).await {
				Ok(Some(current_state)) => current_state,
				Ok(None) => {
					let _ = tx.tr.cancel().await;
					return Ok(());
				}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err);
				}
			};
			if current.generation != br.generation
				|| !matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				let _ = tx.tr.cancel().await;
				return Ok(());
			}

			let mut next = current;
			next.phase = IndexBuildPhase::Error;
			next.owner = None;
			next.owner_heartbeat_at = None;
			next.updated_at = Utc::now();
			next.error = Some(reason.clone());
			next.report_status = Some(IndexBuildReportStatus::Error);

			match tx.set_key(&bs, &next).await {
				Ok(()) => {}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err);
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
		let tx = match tf.transaction(TransactionType::Write, sequences).await {
			Ok(tx) => tx,
			Err(_) => return Err(reservations),
		};
		for reservation in &reservations {
			match tx.tr.delc(reservation.key.as_borrowed(), Some(&reservation.val)).await {
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
	pub async fn next_batch<'s>(&'s mut self, limit: u32) -> Result<KeysBatch<'s>> {
		let batch = self.inner.next_batch(limit).await.map_err(Error::from)?;
		self.metrics.record_scan(batch.len() as u64, batch.key_bytes, 0);
		Ok(batch)
	}

	/// Drive the cursor, invoking `f` per key borrowed directly from the
	/// cursor (zero-copy on backends that override `for_each`). Records this
	/// chunk's keys/bytes against the transaction's scan metrics in a single
	/// `record_scan` call, matching `next_batch`'s metric granularity.
	pub async fn for_each(&mut self, limit: u32, f: &mut dyn KeyVisitor) -> Result<ScanChunkStats> {
		let stats = self.inner.for_each(limit, f).await.map_err(Error::from)?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, 0);
		Ok(stats)
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
	pub async fn next_batch<'s>(&'s mut self, limit: u32) -> Result<ValsBatch<'s>> {
		let batch = self.inner.next_batch(limit).await.map_err(Error::from)?;
		self.metrics.record_scan(batch.len() as u64, batch.key_bytes, batch.value_bytes);
		Ok(batch)
	}

	/// Drive the cursor, invoking `f` per `(key, value)` borrowed directly from
	/// the cursor (zero-copy on backends that override `for_each`). Records this
	/// chunk's keys/bytes against the transaction's scan metrics in a single
	/// `record_scan` call, matching `next_batch`'s metric granularity.
	pub async fn for_each(&mut self, limit: u32, f: &mut dyn ValVisitor) -> Result<ScanChunkStats> {
		let stats = self.inner.for_each(limit, f).await.map_err(Error::from)?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(stats)
	}
}

/// Database-wide summary of which tables any `REFERENCE` field can target,
/// memoized per transaction for the DELETE reference-purge gate
/// ([`Transaction::table_may_have_incoming_references`]).
struct ReferenceTargets {
	/// Some reference field can hold a record of *any* table (an untyped
	/// `record`), so every table must be treated as potentially referenced.
	any: bool,
	/// The concrete set of tables that typed reference fields can target.
	tables: HashSet<TableName>,
}

impl ReferenceTargets {
	fn can_target(&self, table: &TableName) -> bool {
		self.any || self.tables.contains(table)
	}
}

impl Transaction {
	/// Returns `true` if any `DEFINE FIELD ... REFERENCE` in this database could
	/// target a record in `table` (so a record in `table` may have incoming
	/// reference keys).
	///
	/// A reference key is only ever written under a record's range while some
	/// reference field can hold a record id of that record's table, so when no
	/// reference field can target `table` there can be no reference keys for any
	/// record in it. The DELETE purge path uses this to skip the per-record
	/// reference range scan entirely in that case — on a distributed backend
	/// that scan is a read round-trip per deleted record.
	///
	/// The per-database answer is derived from the (transaction-cached) table
	/// and field catalog and memoized for the transaction, so a batch delete
	/// computes it at most once regardless of how many records or tables it
	/// touches. It is conservative: any reference field whose kind is not
	/// provably unable to hold a record of `table` keeps the scan, so a record
	/// that genuinely needs its references cleaned is never skipped.
	pub(crate) async fn table_may_have_incoming_references(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		table: &TableName,
	) -> Result<bool> {
		Ok(self.database_reference_targets(ns, db).await?.can_target(table))
	}

	/// Compute, or fetch the memoized, [`ReferenceTargets`] summary for a
	/// database. Invalidated alongside field definitions (see `put_tb_field`
	/// and the `clear_cache` on ALTER/REMOVE FIELD).
	async fn database_reference_targets(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<ReferenceTargets>> {
		let qey = cache::tx::Lookup::DbReferenceTargets(ns, db);
		if let Some(entry) = self.cache.get(&qey) {
			return entry.try_into_type::<ReferenceTargets>();
		}
		let mut any = false;
		let mut tables = HashSet::new();
		for tb in self.all_tb(ns, db, None).await?.iter() {
			for fd in self.all_tb_fields(ns, db, &tb.name, None).await?.iter() {
				// Only reference fields write reference keys.
				if fd.reference.is_none() {
					continue;
				}
				match &fd.field_kind {
					Some(kind) => {
						if kind.collect_reference_target_tables(&mut tables) {
							any = true;
						}
					}
					// A reference field always has a record-like kind (enforced
					// at DEFINE FIELD time); treat a missing kind as able to
					// target anything, erring towards running the scan.
					None => any = true,
				}
			}
		}
		let targets = Arc::new(ReferenceTargets {
			any,
			tables,
		});
		self.cache.insert(qey, cache::tx::Entry::Any(targets.clone()));
		Ok(targets)
	}

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
			live_events: OnceLock::new(),
			async_event_trigger,
			trigger_async_event: AtomicBool::new(false),
			write_keys_limit: OnceLock::new(),
			write_guard_poisoned: AtomicBool::new(false),
			guarded_writes: AtomicU64::new(0),
			pending_index_build_reservations: Mutex::new(Vec::new()),
			cached_index_build_reservations: Mutex::new(HashMap::new()),
			pending_index_builder_aborts: Mutex::new(Vec::new()),
			pending_uncommitted_index_builds: Mutex::new(Vec::new()),
		}
	}

	/// Arms the write-cardinality guard: once the transaction has buffered
	/// `limit` individual key writes, every further write fails with
	/// [`crate::err::Error::TransactionWriteKeysExceeded`], so a statement's
	/// physical fan-out (cascaded deletes, index maintenance, graph-edge
	/// cleanup) stops accumulating at the bound and the transaction rolls
	/// back atomically. `None` leaves the transaction unbounded.
	///
	/// Armed on every statement-execution path (executor transactions,
	/// externally-supplied client-owned transactions, record-access clause
	/// evaluation); internal maintenance transactions (index builds,
	/// compaction, garbage collection) are created without a limit and are
	/// never guarded. Every write reserves one slot atomically before it is
	/// issued, so concurrent writes within the transaction can never admit
	/// more than the limit. The accounting rules (range deletes, commit-time
	/// feed writes, reservations never being refunded) are documented on
	/// `transaction_max_write_keys` in [`surrealdb_cnf::CommonConfig`].
	pub fn with_write_keys_limit(self, limit: Option<NonZeroU64>) -> Self {
		self.arm_write_keys_limit(limit);
		self
	}

	/// Arms the write-cardinality guard on a transaction that is already
	/// wrapped in an `Arc` — the externally-supplied (client-owned)
	/// transactions that statements execute on via
	/// [`crate::kvs::Datastore::process_with_transaction`] and its variants.
	/// Same contract as [`Self::with_write_keys_limit`]. Idempotent: the
	/// first arming wins and later calls are silently ignored, so repeated
	/// statement executions on one transaction keep a single limit.
	pub fn arm_write_keys_limit(&self, limit: Option<NonZeroU64>) {
		if let Some(limit) = limit {
			let _ = self.write_keys_limit.set(limit);
		}
	}

	/// Reserves one write slot against the write-cardinality guard, failing
	/// when the transaction has already reserved the configured maximum.
	/// Called before every write operation; the reservation is atomic
	/// (compare-and-increment), so writes issued concurrently on the same
	/// transaction each take a distinct slot and the limit holds under any
	/// interleaving.
	fn reserve_write_slot(&self) -> Result<()> {
		if let Some(limit) = self.write_keys_limit.get()
			&& self
				.guarded_writes
				.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
					(n < limit.get()).then_some(n + 1)
				})
				.is_err()
		{
			// Poison the transaction: the writes already buffered are a
			// partial statement, so a later explicit COMMIT must be refused
			// (see the check at the top of [`Self::commit`]).
			self.write_guard_poisoned.store(true, Ordering::Relaxed);
			return Err(crate::err::Error::TransactionWriteKeysExceeded {
				limit: limit.get(),
			}
			.into());
		}
		Ok(())
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
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
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
		// Clear any buffered live-query events
		if let Some(live_events) = self.live_events.get() {
			live_events.clear();
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
		// A tripped write-cardinality guard poisons the transaction: the
		// writes buffered before the failing reservation are a partial
		// statement, so committing them — reachable when a client-owned
		// (RPC/SDK) transaction issues an explicit COMMIT after an
		// over-limit statement error — would break the guard's atomic
		// rollback contract. Refuse the commit and roll back instead;
		// an explicit CANCEL behaves as normal.
		if self.write_guard_poisoned.load(Ordering::Relaxed) {
			let limit = self.write_keys_limit.get().map(|l| l.get()).unwrap_or_default();
			if let Err(err) = self.cancel().await {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"transaction cleanup failed after a poisoned-guard commit was refused: {err}"
				);
			}
			return Err(crate::err::Error::TransactionWriteKeysExceeded {
				limit,
			}
			.into());
		}
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
			// A commit refused because the datastore is shutting down was
			// rejected before it applied (the engine gate blocks it ahead of
			// `inner.commit()`), so the cleanup below is correct: nothing was
			// written. All other cleanup writes it triggers are refused the
			// same way while shutdown is in progress, so a shutting-down
			// datastore stays consistent without special-casing here.
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
	pub async fn exists_key<K>(&self, key: &K, version: Option<u64>) -> Result<bool>
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
	pub async fn get_key<K>(
		&self,
		key: &K,
		version: Option<u64>,
	) -> Result<Option<<K as KVKey>::Value>>
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
		val.map(|v| <K as KVKey>::Value::kv_decode_value(&v, key.value_context())).transpose()
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get(&self, key: Key<'_>, version: Option<u64>) -> Result<Option<Vec<u8>>> {
		let key_bytes = key.len() as u64;
		let val = self.tr.get(key, version).await.map_err(Error::from)?;
		let (keys_found, value_bytes) = match &val {
			Some(v) => (1, v.len() as u64),
			None => (0, 0),
		};
		self.metrics.record_get(keys_found, key_bytes, value_bytes);
		Ok(val)
	}

	/// Fetch a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getr(
		&self,
		key: KeyRange<'_>,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let val = self.tr.getr(key, version).await.map_err(Error::from)?;
		self.metrics.record_get(val.values.len() as u64, val.key_bytes, val.value_bytes);
		Ok(val.values)
	}

	/// Retrieve a batch set of keys from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get_many_key<K>(
		&self,
		keys: Vec<K>,
		version: Option<u64>,
	) -> Result<Vec<Option<<K as KVKey>::Value>>>
	where
		K: KVKey + Debug,
	{
		let encoded_keys: Vec<_> = keys.iter().map(|k| k.encode_key()).collect::<Result<_>>()?;
		let key_bytes: u64 = encoded_keys.iter().map(|k| k.len() as u64).sum();
		let res = self.tr.getm(&encoded_keys, version).await.map_err(Error::from)?;
		self.metrics.record_get(res.records, key_bytes, res.value_bytes);
		res.values
			.into_iter()
			.zip(keys)
			.map(|(v, k)| match v {
				Some(v) => <K as KVKey>::Value::kv_decode_value(&v, k.value_context()).map(Some),
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
	pub async fn get_prefix_key<K>(
		&self,
		key: &K,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
	where
		K: KVRange + Debug,
	{
		let range = key.encode_range()?;
		let res = self.tr.getr(range, version).await.map_err(Error::from)?;

		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);

		Ok(res.values)
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del_key<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		self.tr.del(key).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del(&self, key: Key<'_>) -> Result<()> {
		self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		self.tr.del(key).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del_compare_key<K>(&self, key: &K, chk: Option<&K::Value>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		self.tr.delc(key, chk.as_deref()).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del_compare(&self, key: Key<'_>, chk: Option<&[u8]>) -> Result<()> {
		self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		self.tr.delc(key, chk).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delr(&self, rng: KeyRange<'_>) -> Result<()> {
		self.reserve_write_slot()?;
		self.tr.delr(rng).await.map_err(Error::from)?;
		// Range/prefix deletes don't report the number of affected keys or
		// their byte size.
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del_prefix_key<K>(&self, rng: &K) -> Result<()>
	where
		K: KVRange + Debug,
	{
		let rng = rng.encode_range()?;
		self.delr(rng).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr_key<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		self.tr.clr(key).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr_compare_key<K>(&self, key: &K, chk: Option<&<K as KVKey>::Value>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		self.tr.clrc(key, chk.as_deref()).await.map_err(Error::from)?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrr(&self, rng: KeyRange<'_>) -> Result<()> {
		self.reserve_write_slot()?;
		self.tr.clrr(rng).await.map_err(Error::from)?;
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Delete all versions of a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr_prefix_key<K>(&self, key: &K) -> Result<()>
	where
		K: KVRange,
	{
		self.reserve_write_slot()?;
		let range = key.encode_range()?;
		self.tr.clrr(range).await.map_err(Error::from)?;
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Remove a namespace's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// Unlike [`crate::catalog::providers::NamespaceProvider::del_ns`], this
	/// does **not** delete the (potentially huge) `/*{ns}` data prefix inside
	/// the transaction. Only the catalog name→id entry is removed, so the
	/// namespace is immediately unreachable; a reclaim job is enqueued and
	/// [`crate::kvs::Datastore::reclaim_tombstones`] destroys the data later.
	/// Because both writes are staged in this transaction, a rollback undoes
	/// the removal and never destroys data.
	pub(crate) async fn del_ns_deferred(
		&self,
		ns: &str,
		expunge: bool,
	) -> Result<Option<NamespaceId>> {
		let Some(ns_def) = self.get_ns_by_name(ns, None).await? else {
			return Ok(None);
		};
		// Delete only the catalog definition; defer the data deletion.
		let key = crate::key::root::ns::NamespaceKey {
			ns: Cow::Borrowed(&ns_def.name),
		};
		if expunge {
			self.clr_key(&key).await?;
		} else {
			self.del_key(&key).await?;
		}
		// Enqueue background reclaim of the namespace data prefix.
		let rc = crate::key::root::rc::ReclaimKey::namespace(
			ns_def.namespace_id,
			expunge,
			Uuid::now_v7(),
		);
		self.set_key(
			&rc,
			&crate::key::root::rc::ReclaimState {
				observed_ms: 0,
			},
		)
		.await?;
		// Invalidate cached namespace lookups so the removal is observed.
		self.cache.remove(&cache::tx::Lookup::Nss);
		self.cache.remove(&cache::tx::Lookup::NsByName(&ns_def.name));
		Ok(Some(ns_def.namespace_id))
	}

	/// Remove a database's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// The deferred companion to
	/// [`crate::catalog::providers::DatabaseProvider::del_db`] used by
	/// `REMOVE DATABASE`: the `/*{ns}*{db}` prefix is destroyed later by
	/// [`crate::kvs::Datastore::reclaim_tombstones`], not in this transaction.
	pub(crate) async fn del_db_deferred(
		&self,
		ns: &str,
		db: &str,
		expunge: bool,
	) -> Result<Option<DatabaseId>> {
		let Some(db_def) = self.get_db_by_name(ns, db, None).await? else {
			return Ok(None);
		};
		// Delete only the catalog definition; defer the data deletion.
		let key = crate::key::namespace::db::DatabaseKey {
			ns: db_def.namespace_id,
			db: Cow::Borrowed(&db_def.name),
		};
		if expunge {
			self.clr_key(&key).await?;
		} else {
			self.del_key(&key).await?;
		}
		// Enqueue background reclaim of the database data prefix.
		let rc = crate::key::root::rc::ReclaimKey::database(
			db_def.namespace_id,
			db_def.database_id,
			expunge,
			Uuid::now_v7(),
		);
		self.set_key(
			&rc,
			&crate::key::root::rc::ReclaimState {
				observed_ms: 0,
			},
		)
		.await?;
		// Invalidate cached database lookups so the removal is observed.
		self.cache.remove(&cache::tx::Lookup::Dbs(db_def.namespace_id));
		self.cache.remove(&cache::tx::Lookup::DbByName(ns, &db_def.name));
		Ok(Some(db_def.database_id))
	}

	/// Remove an index's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// The deferred companion to
	/// [`crate::catalog::providers::TableProvider::del_tb_index`] used by
	/// `REMOVE INDEX`. The catalog definition and id→name lookup are removed
	/// immediately so the index stops being maintained and used; the
	/// `/*{ns}*{db}*{tb}+{ix}` data prefix is destroyed later by
	/// [`crate::kvs::Datastore::reclaim_tombstones`].
	///
	/// Safe against index recreation because index ids are never reused: a new
	/// `DEFINE INDEX` of the same name allocates a fresh id (the old definition
	/// is already gone), so its data prefix is disjoint from the one queued for
	/// reclaim here.
	pub(crate) async fn del_tb_index_deferred(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: &str,
	) -> Result<()> {
		let Some(ix_def) = self.get_tb_index(ns, db, tb, ix, None).await? else {
			return Ok(());
		};
		// Delete the catalog definition; defer the index data deletion.
		let key = crate::key::table::ix::IndexDefinitionKey {
			prefix: crate::key::database::all::DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
			ix: Cow::Borrowed(&ix_def.name),
		};
		self.del_key(&key).await?;
		// Delete the id-to-name lookup.
		let name_lookup_key = crate::key::table::ix::IndexNameLookupKey {
			prefix: crate::key::database::all::DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
			ix: ix_def.index_id,
		};
		self.del_key(&name_lookup_key).await?;
		// Enqueue background reclaim of the index data prefix.
		let rc = crate::key::root::rc::ReclaimKey {
			kind: ReclaimKind::Index,

			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix: ix_def.index_id,
			expunge: Expunge::Keep,
			uid: Uuid::now_v7(),
		};
		self.set_key(
			&rc,
			&crate::key::root::rc::ReclaimState {
				observed_ms: 0,
			},
		)
		.await?;
		// Invalidate the cached list of all indexes for this table.
		self.cache.remove(&cache::tx::Lookup::Ixs(ns, db, tb.as_ref()));
		// Invalidate the cached index entry.
		self.cache.remove(&cache::tx::Lookup::Ix(ns, db, tb.as_ref(), &ix_def.name));
		Ok(())
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn set_key<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.set(key, val).await.map_err(Error::from)?;
		self.metrics.record_set(key_bytes, value_bytes);
		Ok(())
	}

	/// Insert or update a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn set<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		let val = val.into_bytes();
		let value_bytes = val.len() as u64;
		self.tr.set(key, val).await.map_err(Error::from)?;
		self.metrics.record_set(key_bytes, value_bytes);
		Ok(())
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put_key<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.put(key, val).await.map_err(Error::from)?;
		self.metrics.record_put(key_bytes, value_bytes);
		Ok(())
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		let val = val.into_bytes();
		let value_bytes = val.len() as u64;
		self.tr.put(key, val).await.map_err(Error::from)?;
		self.metrics.record_put(key_bytes, value_bytes);
		Ok(())
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put_compare_key<K>(
		&self,
		key: &K,
		val: &K::Value,
		chk: Option<&K::Value>,
	) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let chk = chk.map(|v| v.kv_encode_value()).transpose()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.putc(key, val, chk).await.map_err(Error::from)?;
		self.metrics.record_put(key_bytes, value_bytes);
		Ok(())
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn put_compare<V>(&self, key: Key<'_>, val: V, chk: Option<V>) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		self.reserve_write_slot()?;
		let val = val.into_bytes();
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.putc(key, val, chk.map(|x| x.into_bytes())).await.map_err(Error::from)?;
		self.metrics.record_put(key_bytes, value_bytes);
		Ok(())
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn replace_key<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
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
	pub async fn get_key_raw<K>(&self, key: &K, version: Option<u64>) -> Result<Option<Val>>
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
		let res = self.tr.getm(&keys, version).await.map_err(Error::from)?;
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
	pub async fn keys(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		let res = self.tr.keys(rng, limit, skip, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.keys.len() as u64, res.key_bytes, 0);
		Ok(res.keys)
	}

	/// Retrieve a specific range of keys from the datastore in reverse order.
	///
	/// This function fetches the full range of keys, in a single request to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keysr(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		let res = self.tr.keysr(rng, limit, skip, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.keys.len() as u64, res.key_bytes, 0);
		Ok(res.keys)
	}

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scan(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Val)>> {
		let res = self.tr.scan(rng, limit, skip, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// Retrieve a specific range of keys from the datastore, in reverse order.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scanr(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Val)>> {
		let res = self.tr.scanr(rng, limit, skip, version).await.map_err(Error::from)?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total count, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn count(&self, rng: KeyRange<'_>, version: Option<u64>) -> Result<usize> {
		let n = self.tr.count(rng, version).await.map_err(Error::from)?;
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
	pub async fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: ScanDirection,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredKeysCursor<'a>> {
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
	pub async fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: ScanDirection,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredValsCursor<'a>> {
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
	pub async fn batch_keys(
		&self,
		rng: KeyRange<'_>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Vec<u8>>> {
		Ok(self.tr.batch_keys(rng, batch, version).await.map_err(Error::from)?)
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_vals(
		&self,
		rng: KeyRange<'_>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Vec<u8>, Val)>> {
		Ok(self.tr.batch_keys_vals(rng, batch, version).await.map_err(Error::from)?)
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

	/// Get the current safe (closed) watermark timestamp — the versionstamp at or
	/// below which every committed transaction is final and visible. The
	/// live-query router uses this so it never advances past a commit that could
	/// still become visible with a lower versionstamp. Defaults to
	/// [`Self::timestamp`]; distributed backends override it.
	pub async fn safe_timestamp(&self) -> Result<BoxTimeStamp> {
		Ok(self.tr.safe_timestamp().await.map_err(Error::from)?)
	}

	/// Returns `true` if the table has at least one durable live-query
	/// subscription row, read within this transaction's snapshot.
	///
	/// This is the Router-engine change-capture gate. It deliberately reads the
	/// committed `key::table::lq` rows — the cluster-wide source of truth — rather
	/// than any node-local in-memory cache: on a shared/replicated store the
	/// per-node caches have no cross-node invalidation, so a cache could miss a
	/// subscription created on another node and the write would fail to capture an
	/// event that subscriber needs (a loss that can never be replayed). Reading
	/// within the write's own snapshot makes the gate consistent and cluster-wide.
	/// It is a limit-1 key scan, so the cost is independent of the subscriber
	/// count on the table.
	pub(crate) async fn table_has_live_query(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<bool> {
		let range = crate::key::table::lq::LqPrefix {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(tb),
		}
		.encode_range()?;
		Ok(!self.keys(range, 1, 0, None).await?.is_empty())
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

	/// Records a record change into the dedicated live-query event buffer.
	///
	/// Independent of the changefeed: it always retains full before/after values
	/// and is flushed to the `lqe` keyspace at commit (see [`Self::store_changes`]).
	pub(crate) fn live_event_buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordId,
		previous: CursorRecord,
		current: CursorRecord,
	) {
		self.live_events.get_or_init(LiveEventBuffer::new).buffer_record_change(
			ns,
			db,
			tb,
			id.clone(),
			previous.into_owned(),
			current.into_owned(),
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
	///
	/// The changefeed versionstamp is taken from [`Self::timestamp`] (the backend timestamp
	/// oracle). It is process-local-monotonic on mem/rocksdb/surrealkv and globally monotonic
	/// only on backends with a coordinated oracle (TiKV TSO) — so cross-node changefeed ordering
	/// is a property of the backend, not of this engine.
	pub(crate) async fn store_changes(&self) -> Result<()> {
		// Gather buffered changefeed entries (if any).
		let cf_changes = match self.changefeed.get() {
			Some(changefeed) => changefeed.changes()?,
			None => Vec::new(),
		};
		// Gather buffered live-query events (if any).
		let lqe_changes = match self.live_events.get() {
			Some(live_events) => live_events.changes()?,
			None => Vec::new(),
		};
		// Nothing buffered in either keyspace -> nothing to do.
		if cf_changes.is_empty() && lqe_changes.is_empty() {
			return Ok(());
		}
		// Both keyspaces share this commit's versionstamp.
		let buf = &mut [0u8; _];
		let ts = self.timestamp().await?.encode(buf);
		// Write the buffered changefeed entries. These commit-time writes are
		// part of the transaction's write set, so they are metered and
		// checked against the write-cardinality guard like any other write.
		// Writes are issued sequentially so each capacity check observes
		// every previous write — concurrent checks against a stale count
		// could otherwise admit more keys than the configured limit. Entries
		// are few (one per table per keyspace), so sequencing costs no
		// meaningful concurrency.
		for (ns, db, tb, value) in cf_changes {
			// Create the changefeed key with the current timestamp
			let key = crate::key::change::ChangeFeed {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				ts: Cow::Borrowed(ts),
				tb: Cow::Borrowed(&tb),
			}
			.encode_key()?;
			self.reserve_write_slot()?;
			let key_bytes = key.len() as u64;
			let value_bytes = value.len() as u64;
			// Write the changefeed entry using the raw transactor API
			self.tr.set(key, value).await.map_err(Error::from)?;
			self.metrics.record_set(key_bytes, value_bytes);
		}
		// Write the live-query event entries to the dedicated keyspace,
		// metered, guarded, and sequenced like the changefeed writes above.
		for (ns, db, tb, value) in lqe_changes {
			let key = crate::key::lqe::Lqe {
				prefix: DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(&tb),
				ts: Cow::Borrowed(ts),
			}
			.encode_key()?;
			self.reserve_write_slot()?;
			let key_bytes = key.len() as u64;
			let value_bytes = value.len() as u64;
			self.tr.set(key, value).await.map_err(Error::from)?;
			self.metrics.record_set(key_bytes, value_bytes);
		}
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

	/// Bump the given table's `cache_lives_ts`, committing the change with this
	/// transaction. The live-query cache keys on this committed timestamp (see
	/// [`crate::catalog::TableDefinition::cache_lives_ts`]), so callers that
	/// change the set of live queries on a table (LIVE / KILL) must call this in
	/// the same transaction as the live-query row write. The table's committed
	/// IDs are taken from `tb`, so no namespace/database name lookup is needed.
	pub(crate) async fn bump_table_lives_cache(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		let Some(existing) = self.get_tb(ns, db, tb, None).await? else {
			// The table is gone (e.g. removed in the same batch); nothing to do.
			return Ok(());
		};
		let mut updated = (*existing).clone();
		updated.cache_lives_ts = uuid::Uuid::now_v7();
		let key = crate::key::database::tb::TableKey {
			prefix: crate::key::database::all::DatabaseRoot {
				ns: updated.namespace_id,
				db: updated.database_id,
			},
			tb: Cow::Borrowed(&updated.name),
		};
		self.set_key(&key, &updated).await?;
		// Keep the transaction-local by-id table cache consistent with the
		// write, so a re-read within this transaction sees the bumped value.
		let cached = std::sync::Arc::new(updated);
		let lookup = cache::tx::Lookup::Tb(cached.namespace_id, cached.database_id, &cached.name);
		self.cache.insert(
			lookup,
			cache::tx::Entry::Any(
				std::sync::Arc::clone(&cached) as std::sync::Arc<dyn Any + Send + Sync>
			),
		);
		Ok(())
	}

	/// Clears all keys from the transaction cache.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))]
	pub fn clear_cache(&self) {
		self.cache.clear()
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn compact<K>(&self, key: &K) -> Result<()>
	where
		K: KVRange + Debug,
	{
		let range = key.encode_range()?;
		self.tr.inner.compact(Some(range)).await.map_err(Error::from)?;
		Ok(())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn compact_all(&self) -> Result<()> {
		self.tr.inner.compact(None).await.map_err(Error::from)?;
		Ok(())
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
						let range = crate::key::root::nd::NdPrefix {}.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
						let key = crate::key::root::nd::Nd {
							nd: id,
						};
						let val =
							self.get_key(&key, None).await?.ok_or_else(|| Error::NdNotFound {
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
					let key = crate::key::root::root_config::RootConfig {
						ty: Cow::Borrowed("default"),
					};
					let Some(val) = self.get_key(&key, None).await? else {
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
						let key = crate::key::root::root_config::RootConfig {
							ty: Cow::Borrowed(cg),
						};
						if let Some(val) = self.get_key(&key, None).await? {
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
					let range = crate::key::root::ns::NamespacePrefix {}.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Nss;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nss(),
					None => {
						let range = crate::key::root::ns::NamespacePrefix {}.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
				let key = crate::key::root::ns::NamespaceKey {
					ns: Cow::Borrowed(ns),
				};
				let Some(ns) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				return Ok(Some(Arc::new(ns)));
			}
			let qey = cache::tx::Lookup::NsByName(ns);
			match self.cache.get(&qey) {
				Some(val) => val.try_into_type().map(Some),
				None => {
					let key = crate::key::root::ns::NamespaceKey {
						ns: Cow::Borrowed(ns),
					};
					let Some(ns) = self.get_key(&key, None).await? else {
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
			let key = crate::key::root::ns::NamespaceKey {
				ns: Cow::Borrowed(&ns.name),
			};
			self.set_key(&key, &ns).await?;

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
					let range = crate::key::namespace::db::DatabasePrefix {
						ns,
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dbs(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dbs(),
					None => {
						let range = crate::key::namespace::db::DatabasePrefix {
							ns,
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let key = crate::key::namespace::db::DatabaseKey {
						ns: ns.namespace_id,
						db: Cow::Borrowed(db),
					};
					let Some(db_def) = self.get_key(&key, version).await? else {
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

						let key = crate::key::namespace::db::DatabaseKey {
							ns: ns.namespace_id,
							db: Cow::Borrowed(db),
						};
						let Some(db_def) = self.get_key(&key, None).await? else {
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
			let key = crate::key::namespace::db::DatabaseKey {
				ns: db.namespace_id,
				db: Cow::Borrowed(&db.name),
			};
			self.set_key(&key, &db).await?;

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
					let range = crate::key::database::az::AnalyzerPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Azs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_azs(),
					None => {
						let range = crate::key::database::az::AnalyzerPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::sq::SqPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Sqs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_sqs(),
					None => {
						let range = crate::key::database::sq::SqPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::fc::FcPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Fcs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fcs(),
					None => {
						let range = crate::key::database::fc::FcPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::md::MdPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Mds(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_mds(),
					None => {
						let range = crate::key::database::md::MdPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::pa::PaPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Pas(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_pas(),
					None => {
						let range = crate::key::database::pa::PaPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::ml::MlPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Mls(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_mls(),
					None => {
						let range = crate::key::database::ml::MlPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::cg::ConfigPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Cgs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_cgs(),
					None => {
						let range = crate::key::database::cg::ConfigPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let key = crate::key::database::ml::Ml {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						ml: Cow::Borrowed(ml),
						vn: Cow::Borrowed(vn),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ml(ns, db, ml, vn);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::ml::Ml {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							ml: Cow::Borrowed(ml),
							vn: Cow::Borrowed(vn),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::database::az::Analyzer {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						az: Cow::Borrowed(az),
					};
					let val =
						self.get_key(&key, version).await?.ok_or_else(|| Error::AzNotFound {
							name: az.to_owned(),
						})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Az(ns, db, az);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::database::az::Analyzer {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							az: Cow::Borrowed(az),
						};
						let val =
							self.get_key(&key, None).await?.ok_or_else(|| Error::AzNotFound {
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
					let key = Sq {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						sq: Cow::Borrowed(sq),
					};
					let val =
						self.get_key(&key, version).await?.ok_or_else(|| Error::SeqNotFound {
							name: sq.to_owned(),
						})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Sq(ns, db, sq);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = Sq {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							sq: Cow::Borrowed(sq),
						};
						let val =
							self.get_key(&key, None).await?.ok_or_else(|| Error::SeqNotFound {
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
					let key = crate::key::database::fc::Fc {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						fc: Cow::Borrowed(fc),
					};
					let val =
						self.get_key(&key, version).await?.ok_or_else(|| Error::FcNotFound {
							name: format!("fn::{fc}"),
						})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Fc(ns, db, fc);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::database::fc::Fc {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							fc: Cow::Borrowed(fc),
						};
						let val =
							self.get_key(&key, None).await?.ok_or_else(|| Error::FcNotFound {
								name: format!("fn::{fc}"),
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
					let key = crate::key::database::md::Md {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						md: Cow::Borrowed(md),
					};
					let val =
						self.get_key(&key, version).await?.ok_or_else(|| Error::MdNotFound {
							name: md.to_owned(),
						})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Md(ns, db, md);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::database::md::Md {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							md: Cow::Borrowed(md),
						};
						let val =
							self.get_key(&key, None).await?.ok_or_else(|| Error::MdNotFound {
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
					let key = crate::key::database::pa::Pa {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						pa: Cow::Borrowed(pa),
					};
					let val =
						self.get_key(&key, version).await?.ok_or_else(|| Error::PaNotFound {
							name: pa.to_owned(),
						})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Pa(ns, db, pa);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::database::pa::Pa {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							pa: Cow::Borrowed(pa),
						};
						let val =
							self.get_key(&key, None).await?.ok_or_else(|| Error::PaNotFound {
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
					let key = crate::key::database::cg::Config {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						ty: Cow::Borrowed(cg),
					};
					if let Some(val) = self.get_key(&key, version).await? {
						return Ok(Some(Arc::new(val)));
					} else {
						return Ok(None);
					}
				}
				let qey = cache::tx::Lookup::Cg(ns, db, cg);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = crate::key::database::cg::Config {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							ty: Cow::Borrowed(cg),
						};
						if let Some(val) = self.get_key(&key, None).await? {
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
			let key = crate::key::database::fc::Fc {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				fc: Cow::Borrowed(&fc.name),
			};
			self.set_key(&key, fc).await?;

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
			let key = crate::key::database::md::Md {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				md: Cow::Borrowed(name.as_str()),
			};
			self.set_key(&key, md).await?;

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
			let key = crate::key::database::pa::Pa {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				pa: Cow::Borrowed(&pa.name),
			};
			self.set_key(&key, pa).await?;

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
					let range = crate::key::database::tb::TableKeyPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Tbs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_tbs(),
					None => {
						let range = crate::key::database::tb::TableKeyPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::table::ft::FtPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Fts(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fts(),
					None => {
						let range = crate::key::table::ft::FtPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let table_key = crate::key::database::tb::TableKey {
						prefix: crate::key::database::all::DatabaseRoot {
							ns: db_def.namespace_id,
							db: db_def.database_id,
						},
						tb: Cow::Borrowed(tb),
					};
					if let Some(tb_def) = self.get_key(&table_key, version).await? {
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

						let table_key = crate::key::database::tb::TableKey {
							prefix: crate::key::database::all::DatabaseRoot {
								ns: db_def.namespace_id,
								db: db_def.database_id,
							},
							tb: Cow::Borrowed(tb),
						};
						if let Some(tb_def) = self.get_key(&table_key, None).await? {
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
				let key = crate::key::database::tb::TableKey {
					prefix: crate::key::database::all::DatabaseRoot {
						ns: db.namespace_id,
						db: db.database_id,
					},
					tb: Cow::Borrowed(tb),
				};
				let Some(tb) = self.get_key(&key, version).await? else {
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

					let key = crate::key::database::tb::TableKey {
						prefix: crate::key::database::all::DatabaseRoot {
							ns: db.namespace_id,
							db: db.database_id,
						},
						tb: Cow::Borrowed(tb),
					};
					let Some(tb) = self.get_key(&key, None).await? else {
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
			let key = crate::key::database::tb::TableKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns: tb.namespace_id,
					db: tb.database_id,
				},
				tb: Cow::Borrowed(&tb.name),
			};
			match self.set_key(&key, tb).await {
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

			let key = crate::key::database::tb::TableKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns: tb.namespace_id,
					db: tb.database_id,
				},
				tb: Cow::Borrowed(&tb.name),
			};
			self.del_key(&key).await?;

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

			let key = crate::key::database::tb::TableKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns: tb.namespace_id,
					db: tb.database_id,
				},
				tb: Cow::Borrowed(&tb.name),
			};
			self.clr_key(&key).await?;

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
					let range = crate::key::table::ev::EvPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Evs(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_evs(),
					None => {
						let range = crate::key::table::ev::EvPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::table::fd::FdPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Fds(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fds(),
					None => {
						let range = crate::key::table::fd::FdPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = table_ix::IndexDefinitionPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Ixs(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_ixs(),
					None => {
						let range = table_ix::IndexDefinitionPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::table::lq::LqPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Lvs(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_lvs(),
					None => {
						let range = crate::key::table::lq::LqPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let key = crate::key::database::tb::TableKey {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Tb(ns, db, tb);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::tb::TableKey {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::table::ev::Ev {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
						ev: Cow::Borrowed(ev),
					};
					let val =
						self.get_key(&key, version).await?.ok_or_else(|| Error::EvNotFound {
							name: ev.to_owned(),
						})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Ev(ns, db, tb, ev);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = crate::key::table::ev::Ev {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
							ev: Cow::Borrowed(ev),
						};
						let val =
							self.get_key(&key, None).await?.ok_or_else(|| Error::EvNotFound {
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
					let key = crate::key::table::fd::Fd {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
						fd: Cow::Borrowed(fd),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Fd(ns, db, tb, fd);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::table::fd::Fd {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
							fd: Cow::Borrowed(fd),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
			let key = crate::key::table::fd::Fd {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(tb),
				fd: Cow::Borrowed(&name),
			};
			self.set_key(&key, fd).await?;

			// Invalidate the cached list of all fields for this table
			let list_key = cache::tx::Lookup::Fds(ns, db, tb.as_ref());
			self.cache.remove(&list_key);

			// Defining a field can add (or change) a REFERENCE, so the
			// database-wide reference-target summary memoized for the DELETE
			// purge gate may now be stale. Drop it so it is recomputed on next
			// use. (ALTER/REMOVE FIELD instead clear the whole transaction
			// cache, which covers this entry too.)
			self.cache.remove(&cache::tx::Lookup::DbReferenceTargets(ns, db));

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
					let key = table_ix::IndexDefinitionKey {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
						ix: Cow::Borrowed(ix),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ix(ns, db, tb, ix);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = table_ix::IndexDefinitionKey {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(tb),
							ix: Cow::Borrowed(ix),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
			let key = table_ix::IndexNameLookupKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(tb),
				ix,
			};
			let Some(index_name) = self.get_key(&key, version).await? else {
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
			let key = table_ix::IndexDefinitionKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(tb),
				ix: Cow::Borrowed(&ix.name),
			};
			self.set_key(&key, ix).await?;

			let name_lookup_key = table_ix::IndexNameLookupKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(tb),
				ix: ix.index_id,
			};
			self.set_key(&name_lookup_key, &ix.name.to_string()).await?;

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
			let key = index_all::AllIndexRoot {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(tb),
				ix: ix.index_id,
			};
			self.del_prefix_key(&key).await?;

			// Delete the definition
			let key = table_ix::IndexDefinitionKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(tb),
				ix: Cow::Borrowed(&ix.name),
			};
			self.del_key(&key).await?;

			// Delete the id-to-name lookup
			let name_lookup_key = table_ix::IndexNameLookupKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(tb),
				ix: ix.index_id,
			};
			self.del_key(&name_lookup_key).await?;

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
	/// This function will return a new default initialized record if it does not exist.
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
					let key = crate::key::record::RecordKey {
						root: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						tb: Cow::Borrowed(tb),
						id: Cow::Borrowed(id),
					};
					match self.get_key(&key, version).await? {
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
							let key = crate::key::record::RecordKey {
								root: crate::key::database::all::DatabaseRoot {
									ns,
									db,
								},
								tb: Cow::Borrowed(tb),
								id: Cow::Borrowed(id),
							};
							match self.get_key(&key, None).await? {
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
						.map(|rid| crate::key::record::RecordKey {
							root: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(&rid.table),
							id: Cow::Borrowed(&rid.key),
						})
						.collect();
					let values = self.get_many_key(keys, version).await?;
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
						.map(|(_, rid)| crate::key::record::RecordKey {
							root: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							tb: Cow::Borrowed(&rid.table),
							id: Cow::Borrowed(&rid.key),
						})
						.collect();
					let values = self.get_many_key(keys, None).await?;
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
			let key = crate::key::record::RecordKey {
				root: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(tb),
				id: Cow::Borrowed(id),
			};
			self.exists_key(&key, version).await
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
				let key = crate::key::record::RecordKey {
					root: crate::key::database::all::DatabaseRoot {
						ns,
						db,
					},
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				self.put_key(&key, record.as_ref()).await?;
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
				let key = crate::key::record::RecordKey {
					root: crate::key::database::all::DatabaseRoot {
						ns,
						db,
					},
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				self.set_key(&key, record.as_ref()).await?;
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
				let key = crate::key::record::RecordKey {
					root: crate::key::database::all::DatabaseRoot {
						ns,
						db,
					},
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				self.del_key(&key).await?;
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
					let range = crate::key::root::us::UsPrefix {}.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Rus;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_rus(),
					None => {
						let range = crate::key::root::us::UsPrefix {}.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::namespace::us::UsPrefix {
						ns,
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Nus(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nus(),
					None => {
						let range = crate::key::namespace::us::UsPrefix {
							ns,
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::us::UserKeyPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dus(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dus(),
					None => {
						let range = crate::key::database::us::UserKeyPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let key = crate::key::root::us::Us {
						user: Cow::Borrowed(us),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ru(us);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::root::us::Us {
							user: Cow::Borrowed(us),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::namespace::us::Us {
						ns,
						user: Cow::Borrowed(us),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Nu(ns, us);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::namespace::us::Us {
							ns,
							user: Cow::Borrowed(us),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::database::us::UserKey {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						user: Cow::Borrowed(us),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Du(ns, db, us);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::us::UserKey {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							user: Cow::Borrowed(us),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
			let key = crate::key::root::us::Us {
				user: Cow::Borrowed(&us.name),
			};
			self.set_key(&key, us).await?;

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
			let key = crate::key::namespace::us::Us {
				ns,
				user: Cow::Borrowed(&us.name),
			};
			self.set_key(&key, us).await?;

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
			let key = crate::key::database::us::UserKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				user: Cow::Borrowed(&us.name),
			};
			self.set_key(&key, us).await?;

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
					let range = crate::key::root::ac::AccessKeyPrefix {}.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Ras;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_ras(),
					None => {
						let range = crate::key::root::ac::AccessKeyPrefix {}.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::root::access::gr::AccessGrantPrefix {
						ac: Cow::Borrowed(ra),
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Rgs(ra);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_rag(),
					None => {
						let range = crate::key::root::access::gr::AccessGrantPrefix {
							ac: Cow::Borrowed(ra),
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::namespace::ac::AccessKeyPrefix {
						ns,
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Nas(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nas(),
					None => {
						let range = crate::key::namespace::ac::AccessKeyPrefix {
							ns,
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::namespace::access::gr::AccessGrantKeyPrefix {
						ns,
						ac: Cow::Borrowed(na),
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Ngs(ns, na);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nag(),
					None => {
						let range = crate::key::namespace::access::gr::AccessGrantKeyPrefix {
							ns,
							ac: Cow::Borrowed(na),
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::ac::AccessKeyPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Das(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_das(),
					None => {
						let range = crate::key::database::ac::AccessKeyPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let range = crate::key::database::access::gr::AccessGrantKeyPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						ac: Cow::Borrowed(da),
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dgs(ns, db, da);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dag(),
					None => {
						let range = crate::key::database::access::gr::AccessGrantKeyPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							ac: Cow::Borrowed(da),
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let key = crate::key::root::ac::AccessKey {
						ac: Cow::Borrowed(ra),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ra(ra);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::root::ac::AccessKey {
							ac: Cow::Borrowed(ra),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::root::access::gr::AccessGrantKey {
						ac: Cow::Borrowed(ac),
						gr: Cow::Borrowed(gr),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Rg(ac, gr);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::root::access::gr::AccessGrantKey {
							ac: Cow::Borrowed(ac),
							gr: Cow::Borrowed(gr),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::namespace::ac::AccessKey {
						ns,
						ac: Cow::Borrowed(na),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Na(ns, na);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::namespace::ac::AccessKey {
							ns,
							ac: Cow::Borrowed(na),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::namespace::access::gr::AccessGrantKey {
						ns,
						ac: Cow::Borrowed(ac),
						gr: Cow::Borrowed(gr),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ng(ns, ac, gr);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::namespace::access::gr::AccessGrantKey {
							ns,
							ac: Cow::Borrowed(ac),
							gr: Cow::Borrowed(gr),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::database::ac::AccessKey {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						ac: Cow::Borrowed(da),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Da(ns, db, da);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::ac::AccessKey {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							ac: Cow::Borrowed(da),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
					let key = crate::key::database::access::gr::AccessGrantKey {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						ac: Cow::Borrowed(ac),
						gr: Cow::Borrowed(gr),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Dg(ns, db, ac, gr);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::access::gr::AccessGrantKey {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							ac: Cow::Borrowed(ac),
							gr: Cow::Borrowed(gr),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
			let key = crate::key::root::ac::AccessKey {
				ac: Cow::Borrowed(ra),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = crate::key::root::access::all::AccessRoot {
				ac: Cow::Borrowed(ra),
			};
			self.del_prefix_key(&key).await?;

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
			let key = crate::key::namespace::ac::AccessKey {
				ns,
				ac: Cow::Borrowed(na),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = crate::key::namespace::access::all::AccessRoot {
				ns,
				ac: Cow::Borrowed(na),
			};
			self.del_prefix_key(&key).await?;

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
			let key = crate::key::database::ac::AccessKey {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				ac: Cow::Borrowed(da),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = crate::key::database::access::all::DbAccess {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				ac: Cow::Borrowed(da),
			};
			self.del_prefix_key(&key).await?;

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
					let range = crate::key::database::ap::ApiPrefix {
						ns,
						db,
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Aps(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val,
					None => {
						let range = crate::key::database::ap::ApiPrefix {
							ns,
							db,
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let key = crate::key::database::ap::Api {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						ap: Cow::Borrowed(ap),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Ap(ns, db, ap);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::ap::Api {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							ap: Cow::Borrowed(ap),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
			let key = crate::key::database::ap::Api {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				ap: Cow::Borrowed(&name),
			};
			self.set_key(&key, ap).await?;

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
					let range = crate::key::database::bu::BucketKeyPrefix {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
					}
					.encode_range()?;
					let val = self.tr.getr(range, version).await.map_err(Error::from)?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Bus(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_bus(),
					None => {
						let range = crate::key::database::bu::BucketKeyPrefix {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
						}
						.encode_range()?;
						let val = self.tr.getr(range, None).await.map_err(Error::from)?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
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
					let key = crate::key::database::bu::BucketKey {
						prefix: crate::key::database::all::DatabaseRoot {
							ns,
							db,
						},
						bu: Cow::Borrowed(bu),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(val)));
				}
				let qey = cache::tx::Lookup::Bu(ns, db, bu);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = crate::key::database::bu::BucketKey {
							prefix: crate::key::database::all::DatabaseRoot {
								ns,
								db,
							},
							bu: Cow::Borrowed(bu),
						};
						let Some(val) = self.get_key(&key, None).await? else {
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
