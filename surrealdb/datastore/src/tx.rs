//! Transaction implementation and cache coordination.
//!
//! Cache paths use `Entry::Any(val.clone())` for concrete `Arc<T>` values that must coerce to
//! `Arc<dyn Any + Send + Sync>`; `Arc::clone(&val)` does not perform that unsized coercion.
#![allow(clippy::clone_on_ref_ptr)]
// `Transaction`'s pub methods take `K: KVKey` / `K::Value: KVValue`.
// Both traits are `pub` (their `pub` declarations are gated by
// `pub use` re-exports in `kvs/mod.rs`), so the lint flags every
// such method. The visibility is intentional — silence at module scope.
#![allow(private_bounds, private_interfaces)]

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::ops::{ControlFlow, Deref};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use common::EngineError;
use common::time::sleep;
use roaring::RoaringTreemap;
use surrealdb_catalog::node::Node;
use surrealdb_catalog::providers::{
	ApiProvider, AuthorisationProvider, BoxProviderFut, BucketProvider, CachePolicy,
	CancellationProbe, CatalogProvider, DatabaseProvider, NamespaceProvider, NodeProvider,
	RootProvider, TableProvider, UserProvider,
};
use surrealdb_catalog::{
	self, DatabaseDefinition, DatabaseId, DefaultConfig, Error as CatalogError, FromStored,
	IndexId, LATEST_EDGE_VARIANT, Metadata, NamespaceDefinition, NamespaceId, Record, RecordType,
	StoredConfigDefinition, StoredTableDefinition, TableDefinition, TableId, TableType,
};
use surrealdb_expr::val::{RecordId, RecordIdKey, RecordIdentity, TableName, Value};
use surrealdb_kvs::api::{
	Batch, KeyVisitor, KeysBatch, ScanChunkStats, ScanCursorKeys, ScanCursorVals, ValVisitor,
	ValsBatch,
};
use surrealdb_kvs::key::{AnyRange, KVKey, KVKeyDecode, KVSubspace, Resumable, TypedRange};
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{Key, KeyRange};
use surrealdb_observe::{
	ExecutionObserver, Outcome, TenantIdentity, TransactionEvent, TransactionEventSafe,
	TransactionMetrics,
};
use tokio::sync::Mutex;
use tracing::Instrument;
use uuid::Uuid;
use web_time::Instant;

use crate::cache::tx::TransactionCache;
// The transaction holds the queue because only it knows when the outcome is
// settled; what to run belongs to the layer that registered it.
use crate::close::{CommitAction, RollbackAction};
use crate::key::reclaim::{ALL_DOC_ID_KINDS, Expunge, ReclaimState};
use crate::key::schema::{
	AdjacencyEdgeTablePrefix, AdjacencyKey, AnalyzerKey, AnalyzerPrefix, ApiKey, ApiPrefix,
	BucketKey, BucketPrefix, BuildAppendTicketPrefix, BuildReservationKey, BuildStateKey,
	ChangeFeedKey, ChangefeedFenceKey, DatabaseKey, DatabasePrefix, DbAccessMethodKey,
	DbAccessMethodPrefix, DbAccessRoot, DbConfigKey, DbConfigPrefix, DbGrantKey, DbGrantPrefix,
	DbUserKey, DbUserPrefix, DocKeyKey, DocLookupIdentityKey, DocLookupKey, DocStatsBatchKey,
	EventKey, EventPrefix, FieldKey, FieldPrefix, ForeignTablePrefix, FunctionKey, FunctionPrefix,
	GraphFoldKey, GraphPointerKey, IdxRoot, IndexCompactionKey, IndexCountKey, IndexDefKey,
	IndexDefPrefix, IndexNameKey, LiveEventsKey, MlModelKey, MlModelPrefix, ModuleKey,
	ModulePrefix, NamespaceKey, NamespacePrefix, NodeKey, NodePrefix, NsAccessMethodKey,
	NsAccessMethodPrefix, NsAccessRoot, NsGrantKey, NsGrantPrefix, NsUserKey, NsUserPrefix,
	ParamKey, ParamPrefix, ReclaimKey, ReclaimTbPrefix, RecordKey, RootAccessMethodKey,
	RootAccessMethodPrefix, RootAccessRoot, RootConfigKey, RootGrantKey, RootGrantPrefix,
	RootUserKey, RootUserPrefix, SequenceKey, SequencePrefix, SubscriptionPrefix, TableKey,
	TablePrefix, TermChangeBatchKey,
};
use crate::sequences::{Sequence, Sequences};
#[cfg(any(test, feature = "test-hooks"))]
use crate::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::triggers::CommitTriggers;
use crate::values::changefeed::Changefeed;
use crate::values::fulltext::DocLengthAndCount;
use crate::values::graph::{AdjacencyBlock, BlockEntries, GraphFoldScope};
use crate::values::ids::DocId;
use crate::values::index_build::{
	BuildGeneration, BuildTicket, BuildTicketMutationSeq, IndexBuildPhase, IndexBuildReportStatus,
};
use crate::values::index_delta::{BufferedIndex, BufferedTerm, IndexDeltaBuffer};
use crate::values::live_query::LiveEventBuffer;
use crate::{
	DatastoreError, Direction, Error as KvsError, IntoBytes, NORMAL_BATCH_SIZE, TransactionConfig,
	TransactionFactory, TransactionType, Transactor, Val, cache, catalog,
	is_retryable_transaction_conflict, storage_error, util,
};

/// The slot a transaction holds for a sequence it defined: the allocator it
/// owns for that sequence, empty until something draws from it.
///
/// Its lock is what makes the first draw single-flight. Building the allocator
/// claims a window and writes it through the transaction, so two draws that
/// both found the slot empty would both claim — and the second claim would
/// collide on a row the first created. Whoever takes the lock builds; whoever
/// waits finds it built.
pub(crate) type ProvisionalSequence = Arc<Mutex<Option<Sequence>>>;

/// A transaction's provisional slots, with what each held, at one moment. See
/// `Transaction::provisional_save_points`.
type ProvisionalSnapshot = Vec<((NamespaceId, DatabaseId, String), Option<Sequence>)>;

pub struct Transaction {
	/// Is this is a local datastore transaction?
	local: bool,
	/// The wall-clock instant the transaction was opened. Used to compute
	/// transaction lifetime when emitting the terminal
	/// [`surrealdb_observe::TransactionEvent`].
	started_at: Instant,
	/// Observability hook fired on commit/cancel. Defaults to
	/// [`surrealdb_observe::NoopObserver`] and is otherwise supplied by the
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
	/// the session is known (a client-owned transaction handed to
	/// `Datastore::run`) can still attach identity after the fact via
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
	/// Per-transaction aggregation of count-index deltas and compaction
	/// triggers, flushed as one entry per index inside this transaction's
	/// commit rather than one per mutated document.
	/// Boxed so the buffer's size does not count against every `Transaction`.
	/// It is allocated only for a transaction that actually mutates an index, and
	/// `Transaction` is held on the stack through deep recursive paths where the
	/// inline size matters.
	index_deltas: OnceLock<Box<IndexDeltaBuffer>>,
	/// Post-commit wake-ups, fired once the commit makes the work visible.
	triggers: Arc<CommitTriggers>,
	/// Do we have to trigger async events after the commit?
	trigger_async_event: AtomicBool,
	/// Did this transaction queue any index-compaction work?
	trigger_index_compaction: AtomicBool,
	trigger_graph_fold: AtomicBool,
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
	/// Set when the storage layer could not close a save point.
	///
	/// Which of the scope's writes survive is then unknown, so the buffered index
	/// deltas can be neither kept nor dropped: keeping them risks flushing a term
	/// change for a posting that no longer exists, and dropping them risks a
	/// posting no bitmap names. Their frames can no longer be held at the storage
	/// stack's depth either, so an outer release would fold the wrong one.
	///
	/// Refusing the commit is what makes the choice unnecessary, and it has to be
	/// refused here rather than left to the caller's error handling: a client-owned
	/// (RPC/SDK) transaction survives a statement error and can still issue COMMIT.
	/// Cancelling on the spot would be the more direct answer but is not available
	/// — a caller may hold a cursor, and cancel waits for every cursor to drop,
	/// which that caller cannot do until the call it is blocked in returns.
	save_point_poisoned: AtomicBool,
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
	/// Allocators for sequences this transaction has defined and not committed.
	///
	/// The node's allocator reads definitions through transactions of its own,
	/// so it cannot see a `DEFINE SEQUENCE` this one has written. From the store
	/// alone that is indistinguishable from a definition a concurrent
	/// statement removed or replaced — one wants the caller's view, the other
	/// must not have it — so the statement records itself here, and what gets
	/// built from that view is kept here too rather than in the node's map.
	///
	/// Owning them is what keeps a definition nobody else can see from being
	/// served to anybody else: an allocator built from it has no committed
	/// definition to check its claims against, which is only sound for as long
	/// as this transaction is open. It goes when this transaction goes,
	/// whether that is a commit or a rollback, and a save point rollback puts
	/// it back as it stood when the save point opened — see
	/// [`Self::provisional_save_points`].
	///
	/// A name maps to an empty slot from the moment the statement defines it
	/// until something draws from it.
	provisional_sequences: Mutex<HashMap<(NamespaceId, DatabaseId, String), ProvisionalSequence>>,
	/// Whether [`Self::provisional_sequences`] has ever been written to.
	///
	/// Every `sequence::nextval` asks whether this transaction defined the
	/// sequence it is drawing from, and in all but a `DEFINE SEQUENCE`
	/// transaction the answer is no. This answers that without taking the
	/// lock, so the per-id path pays an atomic load rather than a mutex.
	defines_any_sequence: AtomicBool,
	/// [`Self::provisional_sequences`] as it stood when each open save point
	/// opened, innermost last.
	///
	/// A save point rollback restores the storage it scopes, which includes
	/// every row a provisional allocator wrote — but not the allocators, nor the
	/// record that this transaction defined a sequence, which live here. Left
	/// in place, a `DEFINE SEQUENCE` the rollback undid would still be served
	/// from, on a definition that no longer exists; and an allocator built
	/// inside the scope would keep serving from a window whose claim row is
	/// gone, leaving what it issues unfenced for other nodes, which claim past
	/// the rows they can see. Restoring the snapshot puts both back as they
	/// stood, which is what the storage now says too, so a value drawn inside
	/// the scope is undone along with everything else written there.
	///
	/// The node's own eviction of a redefined sequence is not undone. That costs
	/// the node one rebuild of its allocator, from the definition as committed.
	provisional_save_points: Mutex<Vec<ProvisionalSnapshot>>,
	/// Effects owed once this transaction commits.
	///
	/// They are not transactional - stopping a process-local index builder, or
	/// telling a subscriber its subscription is gone - so running them before the
	/// commit would leave them standing after a rollback undid what they answer
	/// to. Drained by [`Self::commit`]; discarded by [`Self::cancel`] and by a
	/// failed commit.
	commit_actions: Mutex<Vec<Box<dyn CommitAction>>>,
	/// Effects owed if this transaction does *not* commit.
	///
	/// Registered by callers that wrote durable state from a separate transaction
	/// while this one was open, so that state is provisional until this one
	/// commits. `DEFINE INDEX` is the case in point: it starts the builder while
	/// the schema transaction is still open, and the builder may commit build
	/// state and index data of its own before the schema transaction terminates.
	/// Drained by [`Self::cancel`] and by a failed commit; discarded once the
	/// commit succeeds.
	rollback_actions: Mutex<Vec<Box<dyn RollbackAction>>>,
	/// Per-table memo of the settled `graph_doc_ids` marker, backing
	/// [`Self::graph_doc_ids_settled`]. The first read per table is locked
	/// (`getu`), and its conflict registration covers the rest of the
	/// transaction, so later calls — one per record in a multi-record
	/// delete — reuse the answer instead of re-issuing an uncached engine
	/// read each. Cleared by [`Self::rollback_to_save_point`], which may
	/// release the registration on rollback-scoped engines.
	graph_doc_ids_settled: Mutex<HashMap<(NamespaceId, DatabaseId, TableName), bool>>,
}

/// Whether a table's shared doc-ID space is safe for a new consumer to adopt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DocIdsReclaimClaim {
	/// No reclaim had started. Any queued entries were cancelled by this
	/// transaction, so the mappings that remain are internally consistent.
	Available,
	/// At least one reclaim page committed, or an entry of the queued set has
	/// already been retired. The surviving mappings may be torn and must not be
	/// adopted; the queued reclaim is left intact so the background task can
	/// finish it in bounded pages.
	InProgress,
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
pub struct CachedIndexBuildReservationKey {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: TableName,
	pub ix: IndexId,
}

/// Cached admission reservation reused across an entire user transaction.
///
/// First admission for an index runs the short reservation transaction
/// (CAS-incrementing the generation's `!bt` counter and committing `!br`), then stores
/// the resulting `generation`, `ticket`, `initial_complete`, and prepared
/// release here. Subsequent admissions read the cache, take a fresh
/// `mutation_seq`, and write a `!bg` keyed by `(generation, ticket, seq)`.
pub struct CachedIndexBuildReservation {
	pub generation: BuildGeneration,
	pub ticket: BuildTicket,
	pub initial_complete: bool,
	pub next_mutation_seq: BuildTicketMutationSeq,
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
pub enum CachedIndexBuildReservationLookup {
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
pub struct IndexBuildReservationRelease {
	tf: TransactionFactory,
	sequences: Sequences,
	node: Uuid,
	key: Key<'static>,
	val: Val,
}

impl IndexBuildReservationRelease {
	pub fn new(
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

		#[cfg(any(test, feature = "test-hooks"))]
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

		#[cfg(any(test, feature = "test-hooks"))]
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
		let br = BuildReservationKey::decode_key(&self.key)?;
		// One reservation now covers an entire user transaction's mutation
		// batch on this index. Any committed `!bg(generation, ticket, *)`
		// entry signals that at least one mutation in that batch became
		// durable, so the build does not need to be marked errored. Use the
		// inclusive scan range, not a point exists check on `mutation_seq = 0`,
		// because the first mutation may not be at index zero on retry paths
		// that allocate a fresh ticket.
		let range = BuildAppendTicketPrefix {
			ns: br.ns,
			db: br.db,
			tb: Cow::Borrowed(br.tb.as_ref()),
			ix: br.ix,
			generation: br.generation,
			ticket: br.ticket,
		}
		.range()?;

		let bs = BuildStateKey {
			ns: br.ns,
			db: br.db,
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

			match tx.tr.keys(range.clone().into_key_range(), 1, 0, None).await {
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

	pub async fn release(self) -> Result<()> {
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
		let batch = self.inner.next_batch(limit).await?;
		self.metrics.record_scan(batch.len() as u64, batch.key_bytes, 0);
		Ok(batch)
	}

	/// Drive the cursor, invoking `f` per key borrowed directly from the
	/// cursor (zero-copy on backends that override `for_each`). Records this
	/// chunk's keys/bytes against the transaction's scan metrics in a single
	/// `record_scan` call, matching `next_batch`'s metric granularity.
	pub async fn for_each(&mut self, limit: u32, f: &mut dyn KeyVisitor) -> Result<ScanChunkStats> {
		let stats = self.inner.for_each(limit, f).await?;
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
		let batch = self.inner.next_batch(limit).await?;
		self.metrics.record_scan(batch.len() as u64, batch.key_bytes, batch.value_bytes);
		Ok(batch)
	}

	/// Drive the cursor, invoking `f` per `(key, value)` borrowed directly from
	/// the cursor (zero-copy on backends that override `for_each`). Records this
	/// chunk's keys/bytes against the transaction's scan metrics in a single
	/// `record_scan` call, matching `next_batch`'s metric granularity.
	pub async fn for_each(&mut self, limit: u32, f: &mut dyn ValVisitor) -> Result<ScanChunkStats> {
		let stats = self.inner.for_each(limit, f).await?;
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
	/// Route a request path to the one `DEFINE API` that handles it, compiling
	/// only that definition.
	///
	/// Routing is decidable on the stored form (see
	/// [`catalog::StoredApiDefinition::route`]), so this reads the definitions
	/// as stored, picks the most specific match, and compiles just that one.
	/// Reading the compiled list instead would parse every handler body,
	/// fallback and permission clause in the database on every request, of
	/// which at most one is executed — and the caller opens a fresh
	/// transaction per request, so the compiled cache is always cold.
	///
	/// Nothing stored escapes: the return is a compiled definition and the
	/// matched path parameters.
	pub async fn find_db_api(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		segments: &[&str],
		method: catalog::ApiMethod,
	) -> Result<Option<(catalog::ApiDefinition, surrealdb_expr::val::Object)>> {
		let range = ApiPrefix {
			ns,
			db,
		}
		.range()?;
		let val = self.tr.getr(range.into_key_range(), None).await?;
		let stored: Arc<[catalog::StoredApiDefinition]> =
			util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;

		let mut best: Option<(&catalog::StoredApiDefinition, surrealdb_expr::val::Object, u8)> =
			None;
		for api in stored.iter() {
			let Some((params, specificity)) = api.route(segments, method)? else {
				continue;
			};
			if best.as_ref().is_none_or(|(_, _, s)| specificity > *s) {
				best = Some((api, params, specificity));
			}
		}

		let Some((api, params, _)) = best else {
			return Ok(None);
		};
		Ok(Some((catalog::ApiDefinition::from_stored(api)?, params)))
	}

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
	/// The per-database answer is memoized for the transaction, so a batch
	/// delete computes it at most once regardless of how many records or tables
	/// it touches. It is conservative: any reference field whose kind is not
	/// provably unable to hold a record of `table` keeps the scan, so a record
	/// that genuinely needs its references cleaned is never skipped.
	pub async fn table_may_have_incoming_references(
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
		// Walk the stored catalog. Whether a field carries a `REFERENCE` is
		// decidable on the stored form, and almost no field does, so only the
		// `TYPE` text of an actual reference field is compiled.
		//
		// Compiling every definition instead would answer the same boolean while
		// widening the blast radius: this runs from the DELETE purge gate for
		// every record deleted, over every table in the database, and
		// `FieldDefinition::from_stored` propagates on any clause it cannot
		// parse. One unreadable `VALUE` on an unrelated table would then fail
		// every delete in the database. Nothing leaves this function but a
		// boolean and a set of names.
		let tbs: Arc<[StoredTableDefinition]> = {
			let range = TablePrefix {
				ns,
				db,
			}
			.range()?;
			let val = self.tr.getr(range.into_key_range(), None).await?;
			util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?
		};
		for tb in tbs.iter() {
			let tb_name = TableName::from(tb.name.clone());
			let fds: Arc<[catalog::StoredFieldDefinition]> = {
				let range = FieldPrefix {
					ns,
					db,
					tb: Cow::Borrowed(&tb_name),
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), None).await?;
				util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?
			};
			for fd in fds.iter() {
				// Only reference fields write reference keys.
				if fd.reference.is_none() {
					continue;
				}
				match &fd.field_kind {
					Some(kind) => {
						if kind.compile()?.collect_reference_target_tables(&mut tables) {
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

	/// The table's settled `graph_doc_ids` marker — whether numeric
	/// adjacency blocks consume its shared doc-ID space — read through a
	/// locked (`getu`) read of the table-definition key. `false` for a
	/// table with no definition.
	///
	/// The locked read is what makes the answer settled: a plain snapshot
	/// read registers no commit-time conflict on backends that validate
	/// only what a transaction wrote, so a transaction that skips doc-ID
	/// maintenance because it observed the marker unset could commit that
	/// decision after a concurrent fold set the marker (see
	/// `Document::remove_doc_id`). Conflict registration is per key and
	/// validated once at commit, so one locked read covers the whole
	/// transaction: the answer is memoized, and a multi-record delete pays
	/// one engine read per table instead of one per record.
	///
	/// Whether the registration survives a savepoint rollback is
	/// engine-defined (see `Transactable::getu`), so
	/// [`Self::rollback_to_save_point`] clears the memo and the next call
	/// re-issues the locked read.
	pub async fn graph_doc_ids_settled(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<bool> {
		if let Some(&settled) = self.graph_doc_ids_settled.lock().await.get(&(ns, db, tb.clone())) {
			return Ok(settled);
		}
		let key = TableKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let settled = self.getu_key(&key).await?.is_some_and(|stored| stored.graph_doc_ids);
		self.graph_doc_ids_settled.lock().await.insert((ns, db, tb.clone()), settled);
		Ok(settled)
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
		triggers: Arc<CommitTriggers>,
		observer: Arc<dyn ExecutionObserver>,
		tr: Transactor,
		config: &TransactionConfig,
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
			index_deltas: OnceLock::new(),
			triggers,
			trigger_async_event: AtomicBool::new(false),
			trigger_index_compaction: AtomicBool::new(false),
			trigger_graph_fold: AtomicBool::new(false),
			write_keys_limit: OnceLock::new(),
			write_guard_poisoned: AtomicBool::new(false),
			save_point_poisoned: AtomicBool::new(false),
			guarded_writes: AtomicU64::new(0),
			pending_index_build_reservations: Mutex::new(Vec::new()),
			cached_index_build_reservations: Mutex::new(HashMap::new()),
			provisional_sequences: Mutex::new(HashMap::new()),
			defines_any_sequence: AtomicBool::new(false),
			provisional_save_points: Mutex::new(Vec::new()),
			commit_actions: Mutex::new(Vec::new()),
			rollback_actions: Mutex::new(Vec::new()),
			graph_doc_ids_settled: Mutex::new(HashMap::new()),
		}
	}

	/// Arms the write-cardinality guard: once the transaction has buffered
	/// `limit` individual key writes, every further write fails with
	/// [`crate::error::DatastoreError::TransactionWriteKeysExceeded`], so a statement's
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
	/// `TransactionConfig::transaction_max_write_keys`.
	pub fn with_write_keys_limit(self, limit: Option<NonZeroU64>) -> Self {
		self.arm_write_keys_limit(limit);
		self
	}

	/// Arms the write-cardinality guard on a transaction that is already
	/// wrapped in an `Arc` — the externally-supplied (client-owned)
	/// transactions that statements execute on when one is set on a
	/// `Datastore::run` request.
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
			return Err(crate::error::DatastoreError::TransactionWriteKeysExceeded {
				limit: limit.get(),
			}
			.into());
		}
		Ok(())
	}

	/// Attach pre-resolved tenant identity so the emitted
	/// [`TransactionEvent`] carries the active session's namespace,
	/// database, user, session id, and client IP. Typically called by the
	/// [`crate::Datastore`] entry points that create a transaction
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

	/// The counters this transaction has accumulated so far. Only the
	/// commit/cancel event carries them in a normal build; tests read them
	/// mid-transaction to assert what a code path counted.
	#[cfg(feature = "test-hooks")]
	#[doc(hidden)]
	pub fn metrics_snapshot_for_test(&self) -> surrealdb_observe::TransactionMetricsSnapshot {
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
	pub async fn register_index_build_reservation_release(
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
	pub async fn lookup_cached_index_build_reservation(
		&self,
		key: &CachedIndexBuildReservationKey,
	) -> Result<Option<CachedIndexBuildReservationLookup>> {
		let mut cache = self.cached_index_build_reservations.lock().await;
		let Some(entry) = cache.get_mut(key) else {
			return Ok(None);
		};
		let mutation_seq = entry.next_mutation_seq;
		let next_seq = mutation_seq.checked_add(1).ok_or_else(|| {
			DatastoreError::IndexingBuildingCancelled {
				reason: "Per-user-transaction index build mutation sequence overflowed u32::MAX"
					.to_string(),
			}
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
	#[cfg(feature = "test-hooks")]
	#[doc(hidden)]
	pub async fn seed_cached_index_build_reservation_for_test(
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
	pub async fn remove_cached_index_build_reservation(
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
	pub async fn insert_cached_index_build_reservation(
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

	/// Queue work to run once this transaction commits.
	///
	/// See [`Self::commit_actions`] for why an effect waits.
	pub async fn on_commit(&self, action: Box<dyn CommitAction>) {
		self.commit_actions.lock().await.push(action);
	}

	/// Queue work to run if this transaction does not commit.
	///
	/// See [`Self::rollback_actions`] for what belongs here.
	pub async fn on_rollback(&self, action: Box<dyn RollbackAction>) {
		self.rollback_actions.lock().await.push(action);
	}

	/// Records that this transaction has defined `sq`, so a draw from it is
	/// served by this transaction rather than from the node's allocator map.
	/// See [`Self::provisional_sequences`].
	pub async fn sequence_defined_here(&self, ns: NamespaceId, db: DatabaseId, sq: String) {
		self.provisional_sequences.lock().await.insert((ns, db, sq), Arc::new(Mutex::new(None)));
		self.defines_any_sequence.store(true, Ordering::Release);
	}

	/// Captures [`Self::provisional_sequences`] for a save point opening now.
	async fn snapshot_provisional_sequences(&self) -> ProvisionalSnapshot {
		if !self.defines_any_sequence.load(Ordering::Acquire) {
			return Vec::new();
		}
		let held = self.provisional_sequences.lock().await;
		let mut snapshot = Vec::with_capacity(held.len());
		for (key, slot) in held.iter() {
			snapshot.push((key.clone(), slot.lock().await.clone()));
		}
		snapshot
	}

	/// Puts [`Self::provisional_sequences`] back as `snapshot` captured it.
	async fn restore_provisional_sequences(&self, snapshot: ProvisionalSnapshot) {
		if !self.defines_any_sequence.load(Ordering::Acquire) {
			return;
		}
		*self.provisional_sequences.lock().await = snapshot
			.into_iter()
			.map(|(key, allocator)| (key, Arc::new(Mutex::new(allocator))))
			.collect();
	}

	/// This transaction's slot for `sq`, if this is the transaction that
	/// defined it. See [`ProvisionalSequence`].
	pub(crate) async fn provisional_sequence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Option<ProvisionalSequence> {
		if !self.defines_any_sequence.load(Ordering::Acquire) {
			return None;
		}
		self.provisional_sequences.lock().await.get(&(ns, db, sq.to_owned())).cloned()
	}

	/// The sequence allocators this transaction was opened with.
	///
	/// Exposed so a close-time action can open its own transaction from the same
	/// factory: a maintenance transaction needs the same allocators as the one
	/// that registered it.
	pub fn sequences(&self) -> Sequences {
		self.sequences.clone()
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
	/// in a [`surrealdb_kvs::Error::TransactionFinished`] error.
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
		// Discard any buffered index deltas
		if let Some(index_deltas) = self.index_deltas.get() {
			index_deltas.clear();
		}
		// Cancel the underlying transactor. Emit a transaction event on
		// either outcome so counters and durations are always reported
		// even when cancel itself reports a driver-level error.
		//
		// A successful rollback reports [`Outcome::Cancelled`], not
		// `Outcome::Success`: the `outcome` attribute is what separates work
		// that was made durable from work that was discarded, and a commit is
		// the only thing that earns `Success`. `Outcome::Error` stays reserved
		// for a cancel the driver itself failed to perform, which leaves the
		// transaction's durability unknown rather than known-discarded.
		let result = self.tr.cancel().await;
		let cleanup_result = self.run_rollback_actions().await;
		let release_result = self.release_index_build_reservations().await;
		self.discard_commit_actions().await;
		self.emit_transaction_event(match &result {
			Ok(_) => Outcome::Cancelled,
			Err(_) => Outcome::Error,
		});
		result?;
		cleanup_result?;
		release_result?;
		Ok(())
	}

	/// Commit without draining the registered close-time actions.
	///
	/// For the maintenance transactions those actions open themselves: such a
	/// transaction registers nothing, and draining would re-enter the path that
	/// opened it. It emits no transaction event either, so an action's own writes
	/// are not counted as a transaction of the caller's.
	///
	/// It flushes no buffered index deltas either, and refuses rather than drop any
	/// it finds. A transaction that maintained an index has a description of those
	/// writes waiting for [`Self::commit`] to store; committing the writes without
	/// it would leave a record absent from the term bitmaps and statistics that
	/// nothing later reconciles. The transactions this exists for touch keys
	/// directly and buffer nothing, so the check names a caller's mistake rather
	/// than a state to handle.
	///
	/// The poisons are checked for the same reason: a transaction that must not
	/// persist its writes must not persist them by this route either.
	pub async fn commit_bare(&self) -> Result<()> {
		self.poisoned()?;
		if self.index_deltas.get().is_some_and(|buffer| !buffer.is_empty()) {
			return Err(crate::error::DatastoreError::QueryNotExecuted {
				message: "A transaction carrying buffered index deltas cannot be committed bare, \
				          because the deltas describe writes it is about to make durable"
					.to_string(),
			}
			.into());
		}
		Ok(self.tr.commit().await?)
	}

	/// The reason this transaction may not commit, if there is one.
	///
	/// Two conditions poison a transaction, and both mean its writes are a state no
	/// caller asked for:
	///
	/// - A tripped write-cardinality guard leaves the writes buffered before the failing
	///   reservation as a partial statement, so committing them would break the guard's
	///   atomic-rollback contract.
	/// - A save point the storage layer could not close leaves it unknown which of that scope's
	///   writes survive. See [`Self::save_point_poisoned`].
	///
	/// Either is reachable at an explicit COMMIT: a client-owned (RPC/SDK)
	/// transaction survives the statement error that set the flag. An explicit
	/// CANCEL behaves as normal.
	fn poisoned(&self) -> Result<()> {
		if self.write_guard_poisoned.load(Ordering::Relaxed) {
			let limit = self.write_keys_limit.get().map(|l| l.get()).unwrap_or_default();
			return Err(crate::error::DatastoreError::TransactionWriteKeysExceeded {
				limit,
			}
			.into());
		}
		if self.save_point_poisoned.load(Ordering::Relaxed) {
			return Err(crate::error::DatastoreError::QueryNotExecuted {
				message: "A save point could not be closed, leaving the transaction's writes and \
				          the index deltas describing them unreconcilable"
					.to_string(),
			}
			.into());
		}
		Ok(())
	}

	/// Cancel without draining the registered close-time actions. See
	/// [`Self::commit_bare`].
	pub async fn cancel_bare(&self) -> Result<()> {
		Ok(self.tr.cancel().await?)
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn commit(&self) -> Result<()> {
		// Refuse a commit the transaction is no longer allowed to make, and roll
		// back instead. See [`Self::poisoned`].
		if let Err(e) = self.poisoned() {
			if let Err(err) = self.cancel().await {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"transaction cleanup failed after a poisoned commit was refused: {err}"
				);
			}
			return Err(e);
		}
		// Flush the per-index aggregates before the changefeed, so they are
		// part of this transaction's write set. Failure falls into the same
		// cancel-and-report path as the changefeed flush below.
		if let Err(e) = self.store_index_deltas().await {
			if let Err(err) = self.cancel().await {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"transaction cleanup failed after index-delta storage failed; preserving original error {e}: {err}"
				);
			}
			return Err(e);
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
			// Record the write-set size, which is what separates an over-large
			// transaction from an unhealthy cluster and which storage-layer
			// errors do not carry.
			//
			// Outcomes callers expect and already handle are excluded, so
			// ordinary traffic cannot amplify into the level operators alert
			// on: retryable conflicts are re-driven in a loop, `Shutdown`
			// arrives once per draining transaction on every graceful restart,
			// and the conditional-write misses are how `put_compare` and
			// `del_compare` report a lost race, which on last-writer-wins
			// backends surfaces here at commit rather than at the call.
			//
			// The error text is omitted rather than interpolated: backend
			// errors can embed encoded record keys. The error itself reaches
			// the caller, and the backend logs its own cause under this span.
			let expected_outcome = e.is_retryable()
				|| matches!(
					e,
					KvsError::Shutdown
						| KvsError::TransactionConditionNotMet
						| KvsError::TransactionKeyAlreadyExists
				);
			if !expected_outcome {
				let written = self.metrics.snapshot();
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					keys_written = written.keys_written,
					bytes_written = written.total_bytes_written,
					// Write ops only: `ops_total` counts reads as well, which
					// would make a read-heavy transaction look write-heavy.
					write_ops = written
						.ops_put
						.saturating_add(written.ops_set)
						.saturating_add(written.ops_del),
					"transaction commit failed",
				);
			}
			// A commit refused because the datastore is shutting down was
			// rejected before it applied (the engine gate blocks it ahead of
			// `inner.commit()`), so the cleanup below is correct: nothing was
			// written. All other cleanup writes it triggers are refused the
			// same way while shutdown is in progress, so a shutting-down
			// datastore stays consistent without special-casing here.
			let cleanup_result = self.run_rollback_actions().await;
			let release_result = self.release_index_build_reservations().await;
			self.discard_commit_actions().await;
			// The `lq` rows the queued KILLEDs were owed for are still there,
			// so their clients must not be told otherwise. Nothing drains the
			// queue on this path today, but leaving it full makes that a latent
			// hazard rather than an impossibility.
			// Classify the commit failure so the surrealdb.transaction.* metric
			// family can carry an `error_class` attribute. `e` is a concrete
			// `kvs::Error` here -- the transactor's `commit` returns
			// `kvs::Result<()>` (see `kvs/tr.rs`) -- so we apply the
			// kvs-layer rule directly: retryable variants collapse to
			// `txn_conflict`, everything else to `storage`. The shared
			// `classify_anyhow_error` helper applies the same rule from
			// the `anyhow::Error` path used by the executor.
			let class = if e.is_retryable() {
				surrealdb_observe::error_class::TXN_CONFLICT
			} else {
				surrealdb_observe::error_class::STORAGE
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
		self.discard_rollback_actions().await;
		self.run_commit_actions().await;
		if self.trigger_async_event.load(Ordering::Relaxed) {
			// Notify after commit so queued events are visible to workers.
			self.triggers.async_event.notify_one();
		}
		if self.trigger_index_compaction.load(Ordering::Relaxed) {
			// Notify after commit so the queued request is visible to the
			// compactor. Without this the queue waits out the compaction
			// interval, and nothing relates that timer to the write rate — the
			// gap between the two is what lets the delta log grow unbounded.
			self.triggers.index_compaction.notify_one();
		}
		if self.trigger_graph_fold.load(Ordering::Relaxed) {
			// Notify after commit so the queued fold work is visible to the
			// fold task, for the same reason as the compaction trigger above.
			self.triggers.graph_fold.notify_one();
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
		let found = self.tr.exists(key, version).await?;
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
		let val = self.tr.get(encoded, version).await?;
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
		let val = self.tr.get(key, version).await?;
		let (keys_found, value_bytes) = match &val {
			Some(v) => (1, v.len() as u64),
			None => (0, 0),
		};
		self.metrics.record_get(keys_found, key_bytes, value_bytes);
		Ok(val)
	}

	/// Fetch a key from the datastore, registering the key for
	/// commit-time conflict detection.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getu_key<K>(&self, key: &K) -> Result<Option<<K as KVKey>::Value>>
	where
		K: KVKey + Debug,
	{
		// The conflict registration is only validated when the transaction
		// commits, which read-only transactions never do, so a locked read
		// on one must fail rather than silently lose its guarantee.
		if !self.tr.writeable() {
			anyhow::bail!(KvsError::TransactionReadonly);
		}
		let encoded = key.encode_key()?;
		let key_bytes = encoded.len() as u64;
		let val = self.tr.getu(encoded).await?;
		let (keys_found, value_bytes) = match &val {
			Some(v) => (1, v.len() as u64),
			None => (0, 0),
		};
		self.metrics.record_get(keys_found, key_bytes, value_bytes);
		// Build the decode context only on a hit. For `RecordKey` this
		// avoids a `RecordId` clone (table + key) on every miss.
		val.map(|v| <K as KVKey>::Value::kv_decode_value(&v, key.value_context())).transpose()
	}

	/// Fetch a specific record value, registering the record key for
	/// commit-time conflict detection.
	///
	/// This function will return a new default initialized record if it does not exist.
	///
	/// The read is always issued to the underlying datastore, even when
	/// the record is present in the transaction read cache: the conflict
	/// registration must reach the storage engine. The fetched value
	/// refreshes the cache afterwards.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get_record_for_update(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
	) -> Result<Arc<Record>> {
		// Fetch the record from the datastore. `tx.getu_key` decodes
		// using the storage key, so the canonical `id` is spliced back
		// in automatically (see `RecordKey::value_context`).
		let key = RecordKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			id: Cow::Borrowed(id),
		};
		match self.getu_key(&key).await? {
			Some(record) => {
				let record = record.into_read_only();
				let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
				let entry = cache::tx::Entry::Val(Arc::clone(&record));
				self.cache.insert(qey, entry);
				Ok(record)
			}
			None => Ok(Arc::new(Default::default())),
		}
	}

	/// Fetch every entry in a range, with its value decoded under the type the range
	/// says it stores.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getr<V>(
		&self,
		rng: TypedRange<V>,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, V)>>
	where
		V: KVValue<KeyContext = ()> + Send,
	{
		let (out, stats) =
			self.decode_range(rng.into_key_range(), Direction::Forward, None, 0, version).await?;
		self.metrics.record_get(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(out)
	}

	/// Streams a range through the cursor, decoding each value from the bytes the
	/// cursor lends rather than from a copy.
	///
	/// One pass and one output allocation. Nothing materialises the raw rows first,
	/// so the range is never held twice: a backend whose cursor lends borrowed bytes
	/// copies no value at all, and one whose cursor owns its rows owns a batch of
	/// them rather than the whole range.
	///
	/// `limit` bounds the rows read; `None` reads to the end of the range.
	async fn decode_range<V>(
		&self,
		rng: KeyRange<'static>,
		dir: Direction,
		limit: Option<u32>,
		skip: u32,
		version: Option<u64>,
	) -> Result<(Vec<(Vec<u8>, V)>, ScanChunkStats)>
	where
		// `Send` because the decode happens inside the cursor's visitor, which the
		// storage layer requires to be sendable.
		V: KVValue<KeyContext = ()> + Send,
	{
		let mut cursor = self.tr.open_vals_cursor(rng, dir, skip, version).await?;
		let mut out: Vec<(Vec<u8>, V)> = match limit {
			// A bounded read knows its ceiling, so size the output once. Unbounded
			// reads grow, because the range's length is not known until it ends.
			Some(limit) => Vec::with_capacity(limit.min(NORMAL_BATCH_SIZE) as usize),
			None => Vec::new(),
		};
		let mut total = ScanChunkStats::default();
		let mut failed: Option<anyhow::Error> = None;

		let mut remaining = limit;
		loop {
			let chunk = remaining.map_or(NORMAL_BATCH_SIZE, |n| n.min(NORMAL_BATCH_SIZE));
			if chunk == 0 {
				break;
			}
			// The visitor's error type belongs to the storage layer, so a decode
			// failure is carried out here and the cursor is abandoned: after a
			// visitor error its resume position is not defined.
			let stats = cursor
				.for_each(chunk, &mut |key: &[u8], val: &[u8]| match V::kv_decode_value(val, ()) {
					Ok(value) => {
						out.push((key.to_vec(), value));
						Ok(ControlFlow::Continue(()))
					}
					Err(e) => {
						failed = Some(e);
						Ok(ControlFlow::Break(()))
					}
				})
				.await?;
			if let Some(e) = failed {
				return Err(e);
			}
			total.rows += stats.rows;
			total.key_bytes += stats.key_bytes;
			total.value_bytes += stats.value_bytes;
			// A short chunk means the range is exhausted.
			if stats.rows < chunk as u64 {
				break;
			}
			if let Some(n) = remaining.as_mut() {
				*n -= chunk;
			}
		}

		Ok((out, total))
	}

	/// Fetch every entry in a range or region as bytes.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getr_raw(
		&self,
		rng: impl AnyRange,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let val = self.tr.getr(rng.into_key_range(), version).await?;
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
		let res = self.tr.getm(&encoded_keys, version).await?;
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
		K: KVSubspace + Debug,
	{
		let range = key.raw_range()?;
		let res = self.tr.getr(range.into_key_range(), version).await?;

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
		self.tr.del(key).await?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del(&self, key: Key<'_>) -> Result<()> {
		self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		self.tr.del(key).await?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// The stored bytes to retry a failed comparison against, or `None` if the
	/// comparison failed for real.
	///
	/// A revisioned value encodes at whatever revision this build declares,
	/// which need not be the revision the stored value was written at. So a
	/// guarded write whose check value says exactly what is stored is still
	/// refused, and a caller that retries such a write retries forever.
	///
	/// The stored bytes are normalised through the same decode/encode round trip
	/// and compared against `want`. Equal normal forms mean the two values say
	/// the same thing, and the retry then fences on the bytes that are actually
	/// there, so it stays as atomic as the byte comparison it repeats. Anything
	/// else leaves the original refusal standing, as a genuine mismatch must:
	/// a different value, nothing stored, or bytes that will not decode — which
	/// disagree with `want` as surely as a value that decodes to something else.
	///
	/// Called only after a comparison has been refused, so the read it costs is
	/// paid on the failing path rather than on every guarded write.
	///
	/// Both `K::encode_key` and `K::Value::kv_encode_value` must be
	/// deterministic. `want` is a second encoding of the check value the caller
	/// already encoded once, and the retry re-encodes the key and value the
	/// first attempt consumed, so an encoder whose output varied between calls
	/// on one value would compare unequal normal forms and refuse a write that
	/// should succeed. Nothing reached from these value types encodes an
	/// unordered collection — `Object` is a `VecMap` — so the property holds
	/// today, and a value type that breaks it breaks `normalised == want`
	/// regardless of this retry.
	async fn stored_bytes_matching<K>(&self, key: &K, want: &[u8]) -> Result<Option<Val>>
	where
		K: KVKey + Debug,
	{
		let Some(stored) = self.get_key_raw(key, None).await? else {
			return Ok(None);
		};
		let normalised = K::Value::kv_decode_value(&stored, key.value_context())
			.and_then(|v| v.kv_encode_value())
			.ok();
		Ok((normalised.as_deref() == Some(want)).then_some(stored))
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del_compare_key<K>(&self, key: &K, chk: Option<&K::Value>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		self.reserve_write_slot()?;
		let encoded = key.encode_key()?;
		let key_bytes = encoded.len() as u64;
		match chk {
			// Nothing to fence on, so nothing to re-examine.
			None => self.tr.delc(encoded, None).await?,
			Some(chk) => {
				let want = chk.kv_encode_value()?;
				// The first attempt hands its key over rather than copying it; a
				// retry re-encodes, on a path that has already paid for a read.
				match self.tr.delc(encoded, Some(&want)).await {
					Err(e) if matches!(e, KvsError::TransactionConditionNotMet) => {
						let Some(stored) = self.stored_bytes_matching(key, &want).await? else {
							return Err(e.into());
						};
						self.tr.delc(key.encode_key()?, Some(&stored)).await?
					}
					other => other?,
				}
			}
		}
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del_compare(&self, key: Key<'_>, chk: Option<&[u8]>) -> Result<()> {
		self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		self.tr.delc(key, chk).await?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn delr(&self, rng: impl AnyRange) -> Result<()> {
		self.reserve_write_slot()?;
		self.tr.delr(rng.into_key_range()).await?;
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
		K: KVSubspace + Debug,
	{
		let rng = rng.raw_range()?;
		self.delr(rng).await
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clr(&self, key: Key<'_>) -> Result<()> {
		self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		self.tr.clr(key).await?;
		self.metrics.record_del(1, key_bytes);
		Ok(())
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
		self.tr.clr(key).await?;
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
		let encoded = key.encode_key()?;
		let key_bytes = encoded.len() as u64;
		match chk {
			// Nothing to fence on, so nothing to re-examine.
			None => self.tr.clrc(encoded, None).await?,
			Some(chk) => {
				let want = chk.kv_encode_value()?;
				// The first attempt hands its key over rather than copying it; a
				// retry re-encodes, on a path that has already paid for a read.
				match self.tr.clrc(encoded, Some(&want)).await {
					Err(e) if matches!(e, KvsError::TransactionConditionNotMet) => {
						let Some(stored) = self.stored_bytes_matching(key, &want).await? else {
							return Err(e.into());
						};
						self.tr.clrc(key.encode_key()?, Some(&stored)).await?
					}
					other => other?,
				}
			}
		}
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn clrr(&self, rng: impl AnyRange) -> Result<()> {
		self.reserve_write_slot()?;
		self.tr.clrr(rng.into_key_range()).await?;
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
		K: KVSubspace,
	{
		self.reserve_write_slot()?;
		let range = key.raw_range()?;
		self.tr.clrr(range.into_key_range()).await?;
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Remove a namespace's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// Unlike [`surrealdb_catalog::providers::NamespaceProvider::del_ns`], this
	/// does **not** delete the (potentially huge) `/*{ns}` data prefix inside
	/// the transaction. Only the catalog name→id entry is removed, so the
	/// namespace is immediately unreachable; a reclaim job is enqueued and
	/// [`crate::Datastore::reclaim_tombstones`] destroys the data later.
	/// Because both writes are staged in this transaction, a rollback undoes
	/// the removal and never destroys data.
	pub async fn del_ns_deferred(&self, ns: &str, expunge: bool) -> Result<Option<NamespaceId>> {
		let Some(ns_def) = self.get_ns_by_name(ns, None).await? else {
			return Ok(None);
		};
		// Delete only the catalog definition; defer the data deletion.
		let key = NamespaceKey {
			ns: Cow::Borrowed(&ns_def.name),
		};
		if expunge {
			self.clr_key(&key).await?;
		} else {
			self.del_key(&key).await?;
		}
		// Enqueue background reclaim of the namespace data prefix.
		let rc = ReclaimKey::namespace(ns_def.namespace_id, expunge, Uuid::now_v7());
		self.set_key(&rc, &crate::key::reclaim::ReclaimState::enqueued()).await?;
		// Invalidate cached namespace lookups so the removal is observed.
		self.cache.remove(&cache::tx::Lookup::Nss);
		self.cache.remove(&cache::tx::Lookup::NsByName(&ns_def.name));
		Ok(Some(ns_def.namespace_id))
	}

	/// Remove a database's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// The deferred companion to
	/// [`surrealdb_catalog::providers::DatabaseProvider::del_db`] used by
	/// `REMOVE DATABASE`: the `/*{ns}*{db}` prefix is destroyed later by
	/// [`crate::Datastore::reclaim_tombstones`], not in this transaction.
	pub async fn del_db_deferred(
		&self,
		ns: &str,
		db: &str,
		expunge: bool,
	) -> Result<Option<DatabaseId>> {
		let Some(db_def) = self.get_db_by_name(ns, db, None).await? else {
			return Ok(None);
		};
		// Delete only the catalog definition; defer the data deletion.
		let key = DatabaseKey {
			ns: db_def.namespace_id,
			db: Cow::Borrowed(&db_def.name),
		};
		if expunge {
			self.clr_key(&key).await?;
		} else {
			self.del_key(&key).await?;
		}
		// Enqueue background reclaim of the database data prefix.
		let rc =
			ReclaimKey::database(db_def.namespace_id, db_def.database_id, expunge, Uuid::now_v7());
		self.set_key(&rc, &crate::key::reclaim::ReclaimState::enqueued()).await?;
		// Invalidate cached database lookups so the removal is observed.
		self.cache.remove(&cache::tx::Lookup::Dbs(db_def.namespace_id));
		self.cache.remove(&cache::tx::Lookup::DbByName(ns, &db_def.name));
		Ok(Some(db_def.database_id))
	}

	/// Remove an index's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// The deferred companion to
	/// [`surrealdb_catalog::providers::TableProvider::del_tb_index`] used by
	/// `REMOVE INDEX`. The catalog definition and id→name lookup are removed
	/// immediately so the index stops being maintained and used; the
	/// `/*{ns}*{db}*{tb}+{ix}` data prefix is destroyed later by
	/// [`crate::Datastore::reclaim_tombstones`].
	///
	/// Safe against index recreation because index ids are never reused: a new
	/// `DEFINE INDEX` of the same name allocates a fresh id (the old definition
	/// is already gone), so its data prefix is disjoint from the one queued for
	/// reclaim here.
	pub async fn del_tb_index_deferred(
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
		let key = IndexDefKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix: Cow::Borrowed(&ix_def.name),
		};
		self.del_key(&key).await?;
		// Delete the id-to-name lookup.
		let name_lookup_key = IndexNameKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix: ix_def.index_id,
		};
		self.del_key(&name_lookup_key).await?;
		self.enqueue_index_reclaim(ns, db, tb, ix_def.index_id, false).await?;
		// Invalidate the cached list of all indexes for this table.
		self.cache.remove(&cache::tx::Lookup::Ixs(ns, db, tb.as_ref()));
		// Invalidate the cached index entry.
		self.cache.remove(&cache::tx::Lookup::Ix(ns, db, tb.as_ref(), &ix_def.name));
		Ok(())
	}

	/// Writes back a table definition produced by modifying a
	/// previously-read copy, serializing the read-modify-write against
	/// every concurrent writer of the same definition.
	///
	/// The definition key is read locked ([`Self::getu_key`]) before the
	/// write. A blind [`TableProvider::put_tb`] is safe only for a
	/// definition's initial write: on a backend whose commit protocol
	/// validates writes but not plain reads (TiKV), two read-modify-writes
	/// of the same definition would both commit and the later one would
	/// silently erase whatever the earlier one changed — most critically
	/// the `graph_folded` marker a concurrent fold flips, whose loss hides
	/// every folded edge on the table. The locked read registers the key
	/// for commit-time validation on every backend, so the second
	/// committer conflicts and retries from a fresh read instead.
	pub async fn replace_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &TableDefinition,
	) -> Result<Arc<TableDefinition>> {
		let key = TableKey {
			ns: tb.namespace_id,
			db: tb.database_id,
			tb: Cow::Borrowed(&tb.name),
		};
		let _ = self.getu_key(&key).await?;
		self.put_tb(ns, db, tb).await
	}

	/// Register this database's changefeed-retention fence for commit-time
	/// conflict detection, so this transaction cannot commit if a retention
	/// change lands first.
	///
	/// Read through [`Self::getu_key`] rather than written: the fence exists only
	/// to give the changefeed collector and the statements that set a retention a
	/// key in common, and a plain read would not serve — on a last-writer-wins
	/// backend nothing validates a read set, which is the backend where the two
	/// otherwise share no key and so never conflict.
	///
	/// The collector calls this inside each page's own write transaction, so the
	/// arming covers that page and no more.
	pub async fn arm_changefeed_retention_fence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		let key = ChangefeedFenceKey {
			ns,
			db,
		};
		let _ = self.getu_key(&key).await?;
		Ok(())
	}

	/// Bump this database's changefeed-retention fence, so a collection page that
	/// armed against it cannot commit.
	///
	/// Called by every path that writes a changefeed retention. The count carries
	/// no meaning beyond changing, and over-bumping costs only a retried page, so
	/// it is bumped whenever a written definition carries a retention rather than
	/// only when one is extended.
	pub async fn bump_changefeed_retention_fence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		let key = ChangefeedFenceKey {
			ns,
			db,
		};
		let next = self.get_key(&key, None).await?.unwrap_or(0).wrapping_add(1);
		self.set_key(&key, &next).await
	}

	/// Enqueue background reclaim of an index-data prefix by its stable id.
	///
	/// The id is sufficient after catalog retirement because index ids are never
	/// reused, so the prefix this names can gain no new writer. `expunge`
	/// preserves the all-version deletion policy of a cascading parent removal.
	pub async fn enqueue_index_reclaim(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		expunge: bool,
	) -> Result<()> {
		let rc = ReclaimKey::index(ns, db, tb, ix, expunge, Uuid::now_v7());
		self.set_key(&rc, &crate::key::reclaim::ReclaimState::enqueued()).await?;
		Ok(())
	}

	/// Enqueue a table's shared doc-ID space for asynchronous background
	/// reclaim.
	///
	/// The deferred companion to
	/// [`surrealdb_idx::docids::TableDocIds::remove_all`], for the callers that
	/// drop the table's last doc-ID-consuming index. The space holds one entry
	/// per record in each direction, so deleting it inline would put a write per
	/// record into the caller's transaction; the prefixes are destroyed later by
	/// [`crate::Datastore::reclaim_tombstones`] instead.
	///
	/// Each prefix is enqueued separately, because the table's key space
	/// interleaves them with the doc-ID sequence counter (`!dd` < `!dh` < `!di` <
	/// `!dj` < `!dp` < `!ds`) and the counter must survive: a monotonic counter is
	/// what stops a later index handing a second record an id the first still
	/// holds. Separate entries also give each prefix its own resume cursor.
	///
	/// The caller must guarantee no doc-ID-consuming index can adopt a surviving
	/// mapping before the reclaim completes. The prefixes are reclaimed
	/// independently and page by page, so an interrupted reclaim leaves the space
	/// torn — a `!di` entry whose `!dd` twin is already gone. Adopting one of
	/// those through
	/// [`resolve_or_assign`](surrealdb_idx::docids::TableDocIds::resolve_or_assign)'s
	/// fast path would give an index entries pointing at a doc-ID that resolves
	/// to no record. `DEFINE INDEX` upholds this with
	/// [`Self::claim_tb_doc_ids_reclaim`], which it calls in the transaction
	/// that defines the consumer.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn del_tb_doc_ids_deferred(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		// All of them in one transaction, so a strict subset surviving in
		// the queue is itself evidence that the reaper has drained and retired
		// the rest. `Self::claim_tb_doc_ids_reclaim` reads that.
		for kind in ALL_DOC_ID_KINDS {
			let rc = ReclaimKey {
				kind,
				ns,
				db,
				tb: Cow::Borrowed(tb),
				// The space is table-scoped: no index id addresses it.
				ix: IndexId(0),
				expunge: Expunge::Keep,
				uid: Uuid::now_v7(),
			};
			self.set_key(&rc, &crate::key::reclaim::ReclaimState::enqueued()).await?;
		}
		Ok(())
	}

	/// Claim a table's shared doc-ID space for a new consumer when it is still
	/// internally consistent.
	///
	/// `DEFINE INDEX` calls this before defining a doc-ID-consuming index, and
	/// the graph fold before recording a table's `graph_doc_ids` marker. If every
	/// entry of the set still exists and none carries a cursor, no deletion has
	/// committed and they are conditionally cancelled in this transaction. The
	/// conditional deletes make the cancellation atomic with a concurrent page on
	/// last-writer-wins backends: either this transaction removes the claim and
	/// defines the consumer, or the page advances the state and this transaction
	/// is rejected.
	///
	/// Once a cursor exists, or a strict subset of the entries survives, the
	/// space may be torn. In that case no entry is cancelled: the caller must not
	/// define a consumer, and the background task remains able to finish the
	/// purge in bounded pages. No queued entries means either no reclaim was ever
	/// needed or it completed, both of which are safe for a new consumer.
	///
	/// Two things say the space is torn. A surviving entry carrying a resume
	/// cursor names a prefix the reaper has deleted from. And because the set —
	/// one entry per kind in [`ALL_DOC_ID_KINDS`] — is enqueued in one
	/// transaction, a strict subset of it surviving means the reaper drained and
	/// retired the rest: a tear no surviving cursor records, since retiring an
	/// entry destroys its cursor with it.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn claim_tb_doc_ids_reclaim(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<DocIdsReclaimClaim> {
		self.tb_doc_ids_reclaim(ns, db, tb, true).await
	}

	/// [`Self::claim_tb_doc_ids_reclaim`] without the cancellation: reports
	/// whether the space could be claimed, writing nothing.
	///
	/// For a caller whose claim decision spans several tables and is
	/// all-or-nothing — probing every table first keeps one unclaimable
	/// table from leaving another table's queued reclaim already cancelled
	/// in the same transaction. Within one transaction a probe's verdict
	/// holds for a following claim: both scan the same snapshot.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn probe_tb_doc_ids_reclaim(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<DocIdsReclaimClaim> {
		self.tb_doc_ids_reclaim(ns, db, tb, false).await
	}

	async fn tb_doc_ids_reclaim(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		cancel: bool,
	) -> Result<DocIdsReclaimClaim> {
		// Whether a surviving entry names a prefix already deleted from, and
		// which of the kinds are still queued for this table, slot for slot with
		// `ALL_DOC_ID_KINDS`.
		let mut started = false;
		let mut queued = [false; ALL_DOC_ID_KINDS.len()];
		// Exact key/value bytes become compare-delete guards if every entry proves
		// untouched. An unreadable value is retained and blocks adoption.
		let mut entries = Vec::new();
		// The queue is keyed by kind first, so a table's entries are not
		// contiguous: each kind is scanned over its own `(kind, ns, db, tb)`
		// range rather than the whole queue being walked to pick them out. Every
		// key such a range holds is one of this table's by construction, so none
		// has to be decoded to be attributed. Each range is still read in batches
		// so an unexpectedly duplicated queue cannot force one unbounded backend
		// scan result; normal operation has one entry per kind.
		for (slot, kind) in ALL_DOC_ID_KINDS.into_iter().enumerate() {
			let mut window = ReclaimTbPrefix {
				kind,
				ns,
				db,
				tb: Cow::Borrowed(tb),
			}
			.range()?;
			loop {
				let items = self.scan_raw(window.clone(), NORMAL_BATCH_SIZE, 0, None).await?;
				let Some((last, _)) = items.last() else {
					break;
				};
				let last = last.clone();
				let exhausted = items.len() < NORMAL_BATCH_SIZE as usize;
				for (k, v) in &items {
					queued[slot] = true;
					// A state that cannot be read is treated as started, because
					// a cursor it may have held cannot be ruled out.
					started |= match ReclaimState::kv_decode_value(v, ()) {
						Ok(state) => state.cursor.is_some(),
						Err(_) => true,
					};
					entries.push((k.clone(), v.clone()));
				}
				if exhausted {
					break;
				}
				window = window.resume_after(&last, Direction::Forward);
			}
		}
		// A strict subset of the set surviving means the reaper drained and
		// retired the rest, so the space is torn whatever the survivors' cursors
		// say. Leave every surviving entry intact: finishing them in bounded pages
		// is the recovery path.
		let queued_any = queued.iter().any(|&q| q);
		let queued_all = queued.iter().all(|&q| q);
		if started || (queued_any && !queued_all) {
			return Ok(DocIdsReclaimClaim::InProgress);
		}
		// No entries means the space was never queued or has been fully drained.
		if !queued_any {
			return Ok(DocIdsReclaimClaim::Available);
		}
		// Every entry is untouched. Withdraw exactly the values observed above so
		// a page that advances one concurrently wins the race rather than having
		// its cursor deleted and its partial purge adopted by the new consumer.
		if cancel {
			for (key, value) in entries {
				self.del_compare(Key::from(key), Some(&value)).await?;
			}
		}
		Ok(DocIdsReclaimClaim::Available)
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
		self.tr.set(key, val).await?;
		self.metrics.record_set(key_bytes, value_bytes);
		Ok(())
	}

	/// Insert or update a key whose write slot was already reserved.
	///
	/// Only for a write the write-cardinality guard was charged for earlier, so
	/// that it is not charged twice. The buffered index deltas are the case:
	/// `buffer_term_change` charges each key the flush will write at the moment
	/// the buffer gains it, which is what lets an over-limit statement report
	/// before its caller commits.
	///
	/// Upsert rather than insert, unlike the count deltas, and the difference is
	/// how many keys each writes. Every engine implements insert as an existence
	/// check followed by a write — a remote round trip per key on TiKV. A count
	/// delta is one key per index, so it can afford to have a colliding
	/// discriminator reported rather than silently absorbed. Term changes are one
	/// key per distinct term, so a read for each would restore the per-key cost the
	/// buffer exists to collapse. What keeps the upsert safe is the discriminator:
	/// a freshly minted v7 UUID per flush, so no key written here is a key another
	/// transaction writes.
	async fn set_key_prereserved<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let val = val.kv_encode_value()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.set(key, val).await?;
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
		self.tr.set(key, val).await?;
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
		self.tr.put(key, val).await?;
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
		self.tr.put(key, val).await?;
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
		let encoded = key.encode_key()?;
		let encoded_val = val.kv_encode_value()?;
		let key_bytes = encoded.len() as u64;
		let value_bytes = encoded_val.len() as u64;
		match chk {
			// Nothing to fence on, so nothing to re-examine.
			None => self.tr.putc(encoded, encoded_val, None).await?,
			Some(chk) => {
				let want = chk.kv_encode_value()?;
				// The first attempt hands its bytes over rather than copying them:
				// it is the path every guarded write takes, and the value can be
				// large. A retry re-derives them, on a path that has already paid
				// for a read.
				match self.tr.putc(encoded, encoded_val, Some(want)).await {
					Err(e) if matches!(e, KvsError::TransactionConditionNotMet) => {
						let want = chk.kv_encode_value()?;
						let Some(stored) = self.stored_bytes_matching(key, &want).await? else {
							return Err(e.into());
						};
						self.tr
							.putc(key.encode_key()?, val.kv_encode_value()?, Some(stored))
							.await?
					}
					other => other?,
				}
			}
		}
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
		self.tr.putc(key, val, chk.map(|x| x.into_bytes())).await?;
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
		self.tr.replace(key, val).await?;
		self.metrics.record_put(key_bytes, value_bytes);
		Ok(())
	}

	// --------------------------------------------------
	// Raw bytes functions
	// --------------------------------------------------

	/// Fetch a key from the datastore, without decoding its value.
	///
	/// The key is still typed; only the value comes back as bytes. That is what a
	/// caller wants when the bytes themselves are the point — a value whose decode
	/// would splice in data from the key that this caller must not have, or one
	/// that has to be compared byte for byte against what is stored. Everything
	/// else should use [`Self::get_key`] and let the key name the value's type.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn get_key_raw<K>(&self, key: &K, version: Option<u64>) -> Result<Option<Val>>
	where
		K: KVKey + Debug,
	{
		let key = key.encode_key()?;
		let key_bytes = key.len() as u64;
		let val = self.tr.get(key, version).await?;
		let (keys_found, value_bytes) = match &val {
			Some(v) => (1, v.len() as u64),
			None => (0, 0),
		};
		self.metrics.record_get(keys_found, key_bytes, value_bytes);
		Ok(val)
	}

	/// Retrieve a batch set of keys from the datastore, without decoding.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn getm_raw<K>(&self, keys: Vec<K>, version: Option<u64>) -> Result<Vec<Option<Val>>>
	where
		K: KVKey + Debug,
	{
		// Consume each typed key as it is encoded so owned fields do not remain
		// resident alongside the complete encoded request during the await.
		let keys = keys.into_iter().map(|k| k.encode_key()).collect::<Result<Vec<_>>>()?;
		let key_bytes: u64 = keys.iter().map(|k| k.len() as u64).sum();
		let res = self.tr.getm(&keys, version).await?;
		self.metrics.record_get(res.records, key_bytes, res.value_bytes);
		Ok(res.values)
	}

	// --------------------------------------------------
	// Range functions
	// --------------------------------------------------

	/// Retrieve the keys in a range.
	///
	/// The keys come back as bytes because a key *is* bytes until it is decoded;
	/// what the typed range guarantees is which key type they all are, so the
	/// caller's `decode_key` cannot be the wrong one.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keys<V>(
		&self,
		rng: TypedRange<V>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		self.keys_raw(rng, limit, skip, version).await
	}

	/// Retrieve the keys in a range, in reverse order.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keysr<V>(
		&self,
		rng: TypedRange<V>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		self.keysr_raw(rng, limit, skip, version).await
	}

	/// Retrieve the keys in a region whose contents are of more than one type.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keys_raw(
		&self,
		rng: impl AnyRange,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		let res = self.tr.keys(rng.into_key_range(), limit, skip, version).await?;
		self.metrics.record_scan(res.keys.len() as u64, res.key_bytes, 0);
		Ok(res.keys)
	}

	/// Retrieve the keys in a region, in reverse order. See [`Self::keys_raw`].
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn keysr_raw(
		&self,
		rng: impl AnyRange,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		let res = self.tr.keysr(rng.into_key_range(), limit, skip, version).await?;
		self.metrics.record_scan(res.keys.len() as u64, res.key_bytes, 0);
		Ok(res.keys)
	}

	/// Retrieve a range of keys and their values, decoded under the type the range
	/// says they store.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scan<V>(
		&self,
		rng: TypedRange<V>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, V)>>
	where
		V: KVValue<KeyContext = ()> + Send,
	{
		let (out, stats) = self
			.decode_range(rng.into_key_range(), Direction::Forward, Some(limit), skip, version)
			.await?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(out)
	}

	/// As [`Self::scan`], in reverse order.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scanr<V>(
		&self,
		rng: TypedRange<V>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, V)>>
	where
		V: KVValue<KeyContext = ()> + Send,
	{
		let (out, stats) = self
			.decode_range(rng.into_key_range(), Direction::Backward, Some(limit), skip, version)
			.await?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(out)
	}

	/// Retrieve a range of keys and their values as bytes.
	///
	/// For a region holding several kinds of key, and for the scan paths that
	/// deliberately work on bytes — the pre-decode filter reads a value's wire form
	/// without building it.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scan_raw(
		&self,
		rng: impl AnyRange,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Val)>> {
		let res = self.tr.scan(rng.into_key_range(), limit, skip, version).await?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// As [`Self::scan_raw`], in reverse order.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn scanr_raw(
		&self,
		rng: impl AnyRange,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Val)>> {
		let res = self.tr.scanr(rng.into_key_range(), limit, skip, version).await?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// Count the keys in a range or region.
	///
	/// Counting reads no values, so it takes either kind of range.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn count(&self, rng: impl AnyRange, version: Option<u64>) -> Result<usize> {
		let n = self.tr.count(rng.into_key_range(), version).await?;
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
	///
	/// The keys come back as bytes because a key *is* bytes until it is decoded;
	/// what the typed range guarantees is which key type they all are, so the
	/// caller's `decode_key` cannot be the wrong one.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn open_keys_cursor<'a, V>(
		&'a self,
		rng: TypedRange<V>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredKeysCursor<'a>> {
		self.open_keys_cursor_raw(rng, dir, skip, version).await
	}

	/// Open a keys-only scan cursor over a region whose contents are of more than
	/// one type. See [`Self::open_keys_cursor`].
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn open_keys_cursor_raw<'a>(
		&'a self,
		rng: impl AnyRange,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredKeysCursor<'a>> {
		let inner = self.tr.open_keys_cursor(rng.into_key_range(), dir, skip, version).await?;
		Ok(MeteredKeysCursor {
			inner,
			metrics: &self.metrics,
		})
	}

	/// Open a stateful key+value scan cursor over a typed range. See
	/// [`Self::open_keys_cursor`].
	///
	/// Both halves are lent as bytes. The typed range fixes which key type and
	/// which value type the region holds, so a caller that defers the decode past
	/// a filter still knows what to decode each surviving row as.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn open_vals_cursor<'a, V>(
		&'a self,
		rng: TypedRange<V>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredValsCursor<'a>> {
		self.open_vals_cursor_raw(rng, dir, skip, version).await
	}

	/// Open a key+value scan cursor over a region whose contents are of more than
	/// one type. See [`Self::open_keys_cursor`].
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn open_vals_cursor_raw<'a>(
		&'a self,
		rng: impl AnyRange,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredValsCursor<'a>> {
		let inner = self.tr.open_vals_cursor(rng.into_key_range(), dir, skip, version).await?;
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
	pub async fn batch_keys<V>(
		&self,
		rng: TypedRange<V>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Vec<u8>>> {
		self.batch_keys_raw(rng, batch, version).await
	}

	/// Retrieve a batched scan over a region whose contents are of more than one
	/// type.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_raw(
		&self,
		rng: impl AnyRange,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Vec<u8>>> {
		Ok(self.tr.batch_keys(rng.into_key_range(), batch, version).await?)
	}

	/// Retrieve a batched scan of keys and values over a specific range of keys in
	/// the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple
	/// requests to the underlying datastore. Both halves are returned as bytes;
	/// the typed range is what says which key type and which value type they are.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_vals<V>(
		&self,
		rng: TypedRange<V>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Vec<u8>, Val)>> {
		self.batch_keys_vals_raw(rng, batch, version).await
	}

	/// Retrieve a batched scan of keys and values over a region whose contents are
	/// of more than one type. See [`Self::batch_keys_vals`].
	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn batch_keys_vals_raw(
		&self,
		rng: impl AnyRange,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Vec<u8>, Val)>> {
		Ok(self.tr.batch_keys_vals(rng.into_key_range(), batch, version).await?)
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	/// The buffered index deltas are scoped alongside the storage save point.
	/// They describe writes this transaction has made, so a rollback that undoes
	/// those writes has to undo the buffered description of them too. So are
	/// the provisional sequences — see [`Self::provisional_save_points`].
	pub async fn new_save_point(&self) -> Result<()> {
		// The storage save point opens first: the buffer's frames mirror its
		// stack, so a failed open must not leave a frame behind for some outer
		// save point's release to pop instead of its own.
		self.inner.new_save_point().await?;
		// Initialised rather than merely inspected: the save point is opened
		// before the mutations it scopes, so waiting for the buffer to exist
		// would leave the first mutation in the transaction's own frame, where a
		// rollback cannot reach it.
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).push_save_point();
		let snapshot = self.snapshot_provisional_sequences().await;
		self.provisional_save_points.lock().await.push(snapshot);
		Ok(())
	}

	/// Release the last save point.
	pub async fn release_last_save_point(&self) -> Result<()> {
		if let Err(e) = self.inner.release_last_save_point().await {
			Self::poison_unless_inert(&e, &self.save_point_poisoned);
			return Err(e.into());
		}
		if let Some(buffer) = self.index_deltas.get() {
			buffer.release_save_point();
		}
		// Released, the scope folds into the enclosing one, whose snapshot
		// still describes the state an outer rollback returns to.
		self.provisional_save_points.lock().await.pop();
		Ok(())
	}

	/// Rollback to the last save point.
	pub async fn rollback_to_save_point(&self) -> Result<()> {
		if let Err(e) = self.inner.rollback_to_save_point().await {
			Self::poison_unless_inert(&e, &self.save_point_poisoned);
			return Err(e.into());
		}
		if let Some(buffer) = self.index_deltas.get() {
			buffer.rollback_save_point();
		}
		// Whether a locked read's conflict registration survives a savepoint
		// rollback is engine-defined, so no memo of one may outlive it: the
		// next `graph_doc_ids_settled` call re-issues the locked read.
		self.graph_doc_ids_settled.lock().await.clear();
		let snapshot = self.provisional_save_points.lock().await.pop();
		if let Some(snapshot) = snapshot {
			self.restore_provisional_sequences(snapshot).await;
		}
		Ok(())
	}

	/// Poison the transaction for a save-point failure that leaves its state in
	/// doubt, which is every one the engine raises after it has begun reverting.
	///
	/// The three below it raises first, from the guards a save-point call opens with:
	/// a scope that is not open, a transaction that cannot be written, and one that is
	/// already closed. Nothing has been reverted in any of them, so the writes stand
	/// exactly as they did and the buffer's frames still mirror the storage stack —
	/// neither side moved. Each reports a caller's mistake it can handle, and killing
	/// its transaction would answer that with one it cannot, replacing a precise error
	/// with a vaguer one.
	///
	/// What must poison is a failure partway through: an engine unwinding a scope that
	/// absorbed released save points reverts once per save point, and one of those
	/// failing leaves it unknown which of the scope's writes survive.
	fn poison_unless_inert(e: &KvsError, poisoned: &AtomicBool) {
		let inert = matches!(
			e,
			KvsError::NoSavepoint | KvsError::TransactionReadonly | KvsError::TransactionFinished
		);
		if !inert {
			poisoned.store(true, Ordering::Relaxed);
		}
	}

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp
	pub async fn timestamp(&self) -> Result<BoxTimeStamp> {
		Ok(self.tr.timestamp().await?)
	}

	/// Get the current safe (closed) watermark timestamp — the versionstamp at or
	/// below which every committed transaction is final and visible. The
	/// live-query router uses this so it never advances past a commit that could
	/// still become visible with a lower versionstamp. Defaults to
	/// [`Self::timestamp`]; distributed backends override it.
	pub async fn safe_timestamp(&self) -> Result<BoxTimeStamp> {
		Ok(self.tr.safe_timestamp().await?)
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
	pub async fn table_has_live_query(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<bool> {
		let range = SubscriptionPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()?;
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
	pub fn changefeed_buffer_table_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		dt: &StoredTableDefinition,
	) {
		self.changefeed.get_or_init(Changefeed::new).buffer_table_change(ns, db, tb, dt)
	}

	/// change will record the change in the changefeed if enabled.
	/// To actually persist the record changes into the underlying kvs,
	/// you must call the `complete_changes` function and then commit the
	/// transaction.
	#[expect(clippy::too_many_arguments)]
	pub fn changefeed_buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordId,
		previous: Arc<Record>,
		current: Arc<Record>,
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

	/// Accumulate a signed count-index delta for this transaction.
	///
	/// Buffered rather than written per document: all of a transaction's
	/// mutations to one index collapse into a single `!iu` entry at commit. The
	/// entry is still a blind write of a key no other transaction shares, so
	/// this keeps the contention-free property the delta log exists for while
	/// removing the per-document fan-out that made the log — and therefore every
	/// `count()` read — grow with write volume.
	pub fn buffer_count_delta(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		delta: i64,
		nid: Uuid,
	) {
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).buffer_count_delta(
			BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			},
			delta,
			nid,
		)
	}

	/// The net count delta this transaction has buffered but not yet written.
	///
	/// The count read path adds this to the committed entries it scans, so a
	/// read still observes its own transaction's uncommitted mutations.
	pub fn pending_count_delta(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> i64 {
		match self.index_deltas.get() {
			Some(buffer) => buffer.pending_count(&BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			}),
			None => 0,
		}
	}

	/// Register that an index wants compaction once this transaction commits.
	///
	/// Deduplicated per index for the same reason as the count deltas: the
	/// queue only needs to name the index once per transaction, not once per
	/// mutated document.
	pub fn buffer_compaction_trigger(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		nid: Uuid,
	) {
		self.index_deltas
			.get_or_init(|| Box::new(IndexDeltaBuffer::new()))
			.buffer_compaction_trigger(
				BufferedIndex {
					ns,
					db,
					tb: tb.clone(),
					ix,
				},
				nid,
			)
	}

	/// Register that a graph adjacency scope wants folding once this
	/// transaction commits. Written by the edge-deletion path when it
	/// tombstones a pointer key on a folded table, so the tombstone is
	/// consumed even if the scope is never traversed again.
	pub fn buffer_graph_fold_trigger(&self, scope: GraphFoldScope, nid: Uuid) {
		self.index_deltas
			.get_or_init(|| Box::new(IndexDeltaBuffer::new()))
			.buffer_graph_fold_trigger(scope, nid)
	}

	/// Record a hot adjacency scope observed by a read, waking the fold
	/// task. Purely in-memory: reads cannot write a queue entry, so the
	/// fold task drains these into the durable queue on its pass.
	pub fn observe_graph_fold(&self, scope: GraphFoldScope) {
		self.triggers.observe_graph_fold(scope);
	}

	/// Record that a document gained (`add`) or lost a full-text term.
	///
	/// Buffered rather than written per (term, document): a `!tt` key per pair
	/// makes the delta log grow with indexed *work*, where one key per distinct
	/// term per transaction bounds it by the vocabulary. The flushed `!tx` entry
	/// is still a blind write of a key no other transaction shares.
	///
	/// The write-cardinality guard is charged here rather than at the flush, once
	/// for each key the flush will write. Charging at the flush would leave an
	/// over-limit statement on a caller-owned transaction reporting nothing until
	/// that caller committed, because nothing else in the statement writes a key
	/// per term any more. The count is the same either way; only when the caller
	/// learns of it differs.
	#[expect(clippy::too_many_arguments, reason = "the index identity is four fields")]
	pub fn buffer_term_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		term: &str,
		doc_id: u64,
		add: bool,
		nid: Uuid,
	) -> Result<()> {
		let fresh =
			self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).buffer_term_change(
				BufferedTerm {
					index: BufferedIndex {
						ns,
						db,
						tb: tb.clone(),
						ix,
					},
					term: term.to_string(),
				},
				doc_id,
				add,
				nid,
			);
		if fresh {
			self.reserve_write_slot()?;
		}
		Ok(())
	}

	/// The document ids this transaction has buffered for one term but not yet
	/// written, as `(added, removed)`.
	///
	/// The full-text read path applies these over the committed entries it scans,
	/// so a query observes the documents its own transaction has just indexed.
	pub fn pending_term_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		term: &str,
	) -> (RoaringTreemap, RoaringTreemap) {
		match self.index_deltas.get() {
			Some(buffer) => buffer.pending_term_change(&BufferedTerm {
				index: BufferedIndex {
					ns,
					db,
					tb: tb.clone(),
					ix,
				},
				term: term.to_string(),
			}),
			None => Default::default(),
		}
	}

	/// Add one document's length to a full-text index's running statistics.
	pub fn buffer_doc_stats(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		stats: DocLengthAndCount,
		nid: Uuid,
	) {
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).buffer_doc_stats(
			BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			},
			stats,
			nid,
		)
	}

	/// The document statistics this transaction has buffered but not yet written,
	/// so a scorer weighs the documents its own transaction has just indexed.
	pub fn pending_doc_stats(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> DocLengthAndCount {
		match self.index_deltas.get() {
			Some(buffer) => buffer.pending_doc_stats(&BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			}),
			None => DocLengthAndCount::default(),
		}
	}

	/// Write the buffered index deltas: one entry per index for the counts,
	/// compaction triggers and statistics, and one per term and direction for the
	/// term changes.
	///
	/// Called from [`Self::commit`] before the underlying commit, so the entries
	/// land in the same transaction as the document changes that produced them
	/// and the count stays atomic with the data. Like the changefeed flush,
	/// these writes are metered against the write-cardinality guard.
	async fn store_index_deltas(&self) -> Result<()> {
		let Some(buffer) = self.index_deltas.get() else {
			return Ok(());
		};
		if buffer.is_empty() {
			return Ok(());
		}
		for (index, delta, nid) in buffer.take_counts() {
			let key = IndexCountKey {
				ns: index.ns,
				db: index.db,
				tb: Cow::Borrowed(&index.tb),
				ix: index.ix,
				uid: Some((nid, Uuid::now_v7())),
				pos: delta > 0,
				count: delta.unsigned_abs(),
			};
			self.put_key(&key, &()).await?;
		}
		let compactions = buffer.take_compactions();
		if !compactions.is_empty() {
			self.trigger_index_compaction();
		}
		for (index, nid) in compactions {
			let key = IndexCompactionKey {
				ns: index.ns,
				db: index.db,
				tb: Cow::Borrowed(&index.tb),
				ix: index.ix,
				nid,
				uid: Uuid::now_v7(),
			};
			self.put_key(&key, &()).await?;
		}
		let graph_folds = buffer.take_graph_folds();
		if !graph_folds.is_empty() {
			self.trigger_graph_fold.store(true, Ordering::Relaxed);
		}
		for (scope, nid) in graph_folds {
			let key = GraphFoldKey {
				ns: scope.ns,
				db: scope.db,
				tb: Cow::Borrowed(&scope.tb),
				id: Cow::Borrowed(&scope.id),
				dir: scope.dir,
				nid,
				uid: Uuid::now_v7(),
			};
			self.put_key(&key, &()).await?;
		}
		// One discriminator for the whole flush. Every key it appears in is
		// already distinct within the flush — `!tx` by term and direction, `!dx`
		// by index — and separate transactions mint separate values, which is
		// what stops one transaction's contributions from overwriting another's.
		//
		// `!dx` is inserted rather than upserted, so a discriminator that did
		// collide is reported instead of silently absorbing one of the two
		// contributions. It can afford the existence check that costs, because it
		// is one key per index; `!tx` is one key per distinct term and cannot, so
		// it upserts and rests on the discriminator alone.
		//
		// Minted on first use: the value costs an entropy syscall, and most
		// commits reaching here carry only count deltas.
		let term_changes = buffer.take_term_changes();
		let doc_stats = buffer.take_doc_stats();
		let uid = if term_changes.is_empty() && doc_stats.is_empty() {
			Uuid::nil()
		} else {
			Uuid::now_v7()
		};
		for (term, delta) in term_changes {
			let index = &term.index;
			for (add, docs) in [(true, delta.added), (false, delta.removed)] {
				if docs.is_empty() {
					continue;
				}
				let key = TermChangeBatchKey {
					ns: index.ns,
					db: index.db,
					tb: Cow::Borrowed(&index.tb),
					ix: index.ix,
					term: Cow::Borrowed(&term.term),
					nid: delta.nid,
					uid,
					add,
				};
				self.set_key_prereserved(&key, &docs).await?;
			}
		}
		for (index, stats, nid) in doc_stats {
			let key = DocStatsBatchKey {
				ns: index.ns,
				db: index.db,
				tb: Cow::Borrowed(&index.tb),
				ix: index.ix,
				nid,
				uid,
			};
			self.put_key(&key, &stats).await?;
		}
		Ok(())
	}

	/// Records a record change into the dedicated live-query event buffer.
	///
	/// Independent of the changefeed: it always retains full before/after values
	/// and is flushed to the `lqe` keyspace at commit (see [`Self::store_changes`]).
	pub fn live_event_buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordId,
		previous: Value,
		current: Value,
	) {
		self.live_events.get_or_init(LiveEventBuffer::new).buffer_record_change(
			ns,
			db,
			tb,
			id.clone(),
			previous,
			current,
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
	pub async fn store_changes(&self) -> Result<()> {
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
			let key = ChangeFeedKey {
				ns,
				db,
				ts: Cow::Borrowed(ts),
				tb: Cow::Borrowed(&tb),
			}
			.encode_key()?;
			self.reserve_write_slot()?;
			let key_bytes = key.len() as u64;
			let value_bytes = value.len() as u64;
			// Write the changefeed entry using the raw transactor API
			self.tr.set(key, value).await?;
			self.metrics.record_set(key_bytes, value_bytes);
		}
		// Write the live-query event entries to the dedicated keyspace,
		// metered, guarded, and sequenced like the changefeed writes above.
		for (ns, db, tb, value) in lqe_changes {
			let key = LiveEventsKey {
				ns,
				db,
				tb: Cow::Borrowed(&tb),
				ts: Cow::Borrowed(ts),
			}
			.encode_key()?;
			self.reserve_write_slot()?;
			let key_bytes = key.len() as u64;
			let value_bytes = value.len() as u64;
			self.tr.set(key, value).await?;
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

	/// Run every queued rollback action.
	///
	/// Every action is attempted even when one fails, and the first error is
	/// returned: each answers to durable state written by a different
	/// transaction, so skipping the rest would leave that state orphaned.
	///
	/// The queue runs one action at a time, so they are all handed the instant
	/// the drain began: an action that waits bounds itself against that, and a
	/// transaction that queued several cannot turn one allowance into several.
	async fn run_rollback_actions(&self) -> Result<()> {
		// Take the queue under the lock to detach it from concurrent registrations
		let actions = {
			let mut pending = self.rollback_actions.lock().await;
			std::mem::take(&mut *pending)
		};
		let drain_started_at = Instant::now();
		let mut first_error = None;
		for action in actions {
			if let Err(err) = action.run(drain_started_at).await
				&& first_error.is_none()
			{
				first_error = Some(err);
			}
		}
		match first_error {
			Some(err) => Err(err),
			None => Ok(()),
		}
	}

	/// Drop the queued rollback actions without running them.
	///
	/// Invoked once the commit has succeeded: the state they would remove is no
	/// longer provisional.
	async fn discard_rollback_actions(&self) {
		self.rollback_actions.lock().await.clear();
	}

	/// Run every queued commit action.
	///
	/// Invoked after a successful commit, so everything they name is durably in
	/// place. Infallible by contract; an action logs its own failures.
	async fn run_commit_actions(&self) {
		let actions = {
			let mut pending = self.commit_actions.lock().await;
			std::mem::take(&mut *pending)
		};
		for action in actions {
			action.run().await;
		}
	}

	/// Drop the queued commit actions without running them.
	///
	/// Invoked on cancel and on commit failure: the change each action answers to
	/// never became durable, so the effect must not happen either.
	async fn discard_commit_actions(&self) {
		self.commit_actions.lock().await.clear();
	}

	// --------------------------------------------------
	// Cache functions
	// --------------------------------------------------

	/// Bump the given table's `cache_lives_ts`, committing the change with this
	/// transaction. The live-query cache keys on this committed timestamp (see
	/// [`surrealdb_catalog::StoredTableDefinition::cache_lives_ts`]), so callers that
	/// change the set of live queries on a table (LIVE / KILL) must call this in
	/// the same transaction as the live-query row write. The table's committed
	/// IDs are taken from `tb`, so no namespace/database name lookup is needed.
	pub async fn bump_table_lives_cache(
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
		let key = TableKey {
			ns: updated.namespace_id,
			db: updated.database_id,
			tb: Cow::Borrowed(tb),
		};
		self.set_key(&key, &updated.to_stored()).await?;
		// Every cached view of this table now carries a stale `cache_lives_ts`:
		// the by-id and by-name definitions, the database's table list, and the
		// table's compiled live-query list. A re-read within this transaction
		// serving any of them would key the datastore live-query cache on the
		// pre-bump timestamp and fan notifications from the pre-bump subscriber
		// list, so the whole transaction cache is dropped. A subset cannot be
		// refreshed in any case: the by-name entry is keyed on the namespace and
		// database *names*, which this call does not have.
		self.clear_cache();
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
		K: KVSubspace + Debug,
	{
		let range = key.raw_range()?;
		self.tr.inner.compact(Some(range.into_key_range())).await?;
		Ok(())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)]
	pub async fn compact_all(&self) -> Result<()> {
		self.tr.inner.compact(None).await?;
		Ok(())
	}

	/// Mark this transaction to wake the async event processor after commit.
	/// Mark that this transaction queued index-compaction work, so the
	/// compactor is woken once the commit makes it visible.
	pub fn trigger_index_compaction(&self) {
		self.trigger_index_compaction.store(true, Ordering::Relaxed);
	}

	pub fn trigger_async_event(&self) {
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
						let range = NodePrefix {}.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
	fn get_node(&self, id: Uuid) -> BoxProviderFut<'_, Result<Option<Arc<Node>>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Nd(id);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = NodeKey {
							nd: id,
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey, val.clone());
						val.try_into_type().map(Option::Some)
					}
				}
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
					let key = RootConfigKey {
						ty: Cow::Borrowed("default"),
					};
					let Some(val) = self.get_key(&key, None).await? else {
						return Ok(None);
					};
					let StoredConfigDefinition::Default(val) = val else {
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
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ConfigDefinition>>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Rcg(cg);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = RootConfigKey {
							ty: Cow::Borrowed(cg),
						};
						if let Some(val) = self.get_key(&key, None).await? {
							let val = Arc::new(catalog::ConfigDefinition::from_stored(&val)?);
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
					let range = NamespacePrefix {}.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Nss;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nss(),
					None => {
						let range = NamespacePrefix {}.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
				let key = NamespaceKey {
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
					let key = NamespaceKey {
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
				None => anyhow::bail!(CatalogError::NsNotFound {
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
			let key = NamespaceKey {
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
		ctx: Option<&'a dyn CancellationProbe>,
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
					let range = DatabasePrefix {
						ns,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dbs(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dbs(),
					None => {
						let range = DatabasePrefix {
							ns,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let key = DatabaseKey {
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

						let key = DatabaseKey {
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
		ctx: Option<&'a dyn CancellationProbe>,
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
									return Err(CatalogError::NsNotFound {
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
		ctx: Option<&'a dyn CancellationProbe>,
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
			let key = DatabaseKey {
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
					let range = AnalyzerPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Azs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_azs(),
					None => {
						let range = AnalyzerPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let range = SequencePrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Sqs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_sqs(),
					None => {
						let range = SequencePrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let range = FunctionPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredFunctionDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Fcs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fcs(),
					None => {
						let range = FunctionPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredFunctionDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = ModulePrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredModuleDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Mds(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_mds(),
					None => {
						let range = ModulePrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredModuleDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = ParamPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredParamDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Pas(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_pas(),
					None => {
						let range = ParamPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredParamDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = MlModelPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredMlModelDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Mls(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_mls(),
					None => {
						let range = MlModelPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredMlModelDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ConfigDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let range = DbConfigPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[StoredConfigDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Cgs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_cgs(),
					None => {
						let range = DbConfigPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[StoredConfigDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let key = MlModelKey {
						ns,
						db,
						ml: Cow::Borrowed(ml),
						vn: Cow::Borrowed(vn),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(catalog::MlModelDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Ml(ns, db, ml, vn);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = MlModelKey {
							ns,
							db,
							ml: Cow::Borrowed(ml),
							vn: Cow::Borrowed(vn),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(catalog::MlModelDefinition::from_stored(&val)?);
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
					let key = AnalyzerKey {
						ns,
						db,
						az: Cow::Borrowed(az),
					};
					let val = self.get_key(&key, version).await?.ok_or_else(|| {
						CatalogError::AzNotFound {
							name: az.to_owned(),
						}
					})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Az(ns, db, az);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = AnalyzerKey {
							ns,
							db,
							az: Cow::Borrowed(az),
						};
						let val = self.get_key(&key, None).await?.ok_or_else(|| {
							CatalogError::AzNotFound {
								name: az.to_owned(),
							}
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
					let key = SequenceKey {
						ns,
						db,
						sq: Cow::Borrowed(sq),
					};
					let val = self.get_key(&key, version).await?.ok_or_else(|| {
						CatalogError::SeqNotFound {
							name: sq.to_owned(),
						}
					})?;
					return Ok(Arc::new(val));
				}
				let qey = cache::tx::Lookup::Sq(ns, db, sq);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = SequenceKey {
							ns,
							db,
							sq: Cow::Borrowed(sq),
						};
						let val = self.get_key(&key, None).await?.ok_or_else(|| {
							CatalogError::SeqNotFound {
								name: sq.to_owned(),
							}
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
					let key = FunctionKey {
						ns,
						db,
						fc: Cow::Borrowed(fc),
					};
					let val = self.get_key(&key, version).await?.ok_or_else(|| {
						CatalogError::FcNotFound {
							name: format!("fn::{fc}"),
						}
					})?;
					return Ok(Arc::new(catalog::FunctionDefinition::from_stored(&val)?));
				}
				let qey = cache::tx::Lookup::Fc(ns, db, fc);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = FunctionKey {
							ns,
							db,
							fc: Cow::Borrowed(fc),
						};
						let val = self.get_key(&key, None).await?.ok_or_else(|| {
							CatalogError::FcNotFound {
								name: format!("fn::{fc}"),
							}
						})?;
						let val = Arc::new(catalog::FunctionDefinition::from_stored(&val)?);
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
					let key = ModuleKey {
						ns,
						db,
						md: Cow::Borrowed(md),
					};
					let val = self.get_key(&key, version).await?.ok_or_else(|| {
						CatalogError::MdNotFound {
							name: md.to_owned(),
						}
					})?;
					return Ok(Arc::new(catalog::ModuleDefinition::from_stored(&val)?));
				}
				let qey = cache::tx::Lookup::Md(ns, db, md);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = ModuleKey {
							ns,
							db,
							md: Cow::Borrowed(md),
						};
						let val = self.get_key(&key, None).await?.ok_or_else(|| {
							CatalogError::MdNotFound {
								name: md.to_owned(),
							}
						})?;
						let val = Arc::new(catalog::ModuleDefinition::from_stored(&val)?);
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
					let key = ParamKey {
						ns,
						db,
						pa: Cow::Borrowed(pa),
					};
					let val = self.get_key(&key, version).await?.ok_or_else(|| {
						CatalogError::PaNotFound {
							name: pa.to_owned(),
						}
					})?;
					return Ok(Arc::new(catalog::ParamDefinition::from_stored(&val)?));
				}
				let qey = cache::tx::Lookup::Pa(ns, db, pa);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = ParamKey {
							ns,
							db,
							pa: Cow::Borrowed(pa),
						};
						let val = self.get_key(&key, None).await?.ok_or_else(|| {
							CatalogError::PaNotFound {
								name: pa.to_owned(),
							}
						})?;
						let val = Arc::new(catalog::ParamDefinition::from_stored(&val)?);
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
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ConfigDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = DbConfigKey {
						ns,
						db,
						ty: Cow::Borrowed(cg),
					};
					if let Some(val) = self.get_key(&key, version).await? {
						return Ok(Some(Arc::new(catalog::ConfigDefinition::from_stored(&val)?)));
					} else {
						return Ok(None);
					}
				}
				let qey = cache::tx::Lookup::Cg(ns, db, cg);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = DbConfigKey {
							ns,
							db,
							ty: Cow::Borrowed(cg),
						};
						if let Some(val) = self.get_key(&key, None).await? {
							let val = Arc::new(catalog::ConfigDefinition::from_stored(&val)?);
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
			let stored = fc.to_stored();
			let key = FunctionKey {
				ns,
				db,
				fc: Cow::Borrowed(&fc.name),
			};
			self.set_key(&key, &stored).await?;

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
			let stored = md.to_stored();
			let name = stored.get_storage_name()?;
			let key = ModuleKey {
				ns,
				db,
				md: Cow::Borrowed(name.as_str()),
			};
			self.set_key(&key, &stored).await?;

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
			let stored = pa.to_stored();
			let key = ParamKey {
				ns,
				db,
				pa: Cow::Borrowed(&pa.name),
			};
			self.set_key(&key, &stored).await?;

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

/// Compile stored live-query subscriptions.
///
/// Every stored subscription appears in the result, including one whose text
/// no longer compiles: that is carried as
/// [`catalog::SubscriptionQuery::Uncompilable`] rather than dropped. A
/// subscription in that state can never match another document, but it still
/// has `lq`/`lv` keys and a client waiting on them, so the statements that tear
/// subscriptions down or report them must still see it. Dropping it here is
/// what previously left such a client waiting on a `KILLED` that could never
/// arrive, and hid the row from the `INFO` an operator would use to find it.
///
/// Compiling must also not fail the reading statement, which is the other half
/// of the same requirement: the subscription belongs to some other client, and
/// every write to the table reads this list, so an error would turn one
/// unreadable row into an outage for the whole table.
///
/// Callers memoize the returned list against the table's `cache_lives_ts`. That
/// cannot hide a subscription that would otherwise be delivered to: compiling is
/// a pure function of the stored bytes, which never change for a given
/// subscription, and the timestamp is bumped whenever the set of subscriptions
/// on the table changes.
fn compile_subscriptions(
	tb: &TableName,
	stored: &[catalog::StoredSubscriptionDefinition],
) -> Arc<[catalog::SubscriptionDefinition]> {
	stored
		.iter()
		.map(|lv| {
			let compiled = catalog::SubscriptionDefinition::compile(lv);
			if matches!(compiled.query, catalog::SubscriptionQuery::Uncompilable { .. }) {
				// Debug, not warn, and without the compile error: this runs on
				// every `Lookup::Lvs` cache miss, which is every mutation of the
				// table, and the condition cannot self-heal because the stored
				// bytes never change. At warn level one uncompilable row emits a
				// line per write.
				//
				// The error is omitted because the parser renders a snippet of
				// the offending source into it, and that is the user's own query
				// text — a `WHERE` clause is exactly where a literal would sit.
				// The id and table are what an operator needs to find and `KILL`
				// it; `INFO ... STRUCTURE` carries the reason.
				tracing::debug!(
					target: "surrealdb::core::kvs",
					subscription_id = %lv.id,
					table = %tb,
					"LIVE subscription is inert: its stored text no longer compiles, so it will \
					 receive no further notifications until it is killed"
				);
			}
			compiled
		})
		.collect()
}

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
					let range = TablePrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[StoredTableDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Tbs(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_tbs(),
					None => {
						let range = TablePrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[StoredTableDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
	) -> BoxProviderFut<'a, Result<Arc<[TableDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let range = ForeignTablePrefix {
						ns,
						db,
						tb: Cow::Borrowed(tb),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[StoredTableDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Fts(ns, db, tb.as_str());
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fts(),
					None => {
						let range = ForeignTablePrefix {
							ns,
							db,
							tb: Cow::Borrowed(tb),
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[StoredTableDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
		ctx: Option<&'a dyn CancellationProbe>,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let Some(db_def) = self.get_db_by_name(ns, db, version).await? else {
						return Err(anyhow::anyhow!(CatalogError::DbNotFound {
							name: db.to_owned(),
						}));
					};
					let table_key = TableKey {
						ns: db_def.namespace_id,
						db: db_def.database_id,
						tb: Cow::Borrowed(tb),
					};
					if let Some(tb_def) = self.get_key(&table_key, version).await? {
						return Ok(Arc::new(TableDefinition::from_stored(&tb_def)?));
					}
					return Err(CatalogError::TbNotFound {
						name: tb.to_owned(),
					}
					.into());
				}
				let qey = cache::tx::Lookup::TbByName(ns, db, tb.as_str());
				match self.cache.get(&qey) {
					// The entry is in the cache
					Some(val) => val.try_into_type(),
					// The entry is not in the cache
					None => {
						let Some(db_def) = self.get_db_by_name(ns, db, None).await? else {
							return Err(anyhow::anyhow!(CatalogError::DbNotFound {
								name: db.to_owned(),
							}));
						};

						let table_key = TableKey {
							ns: db_def.namespace_id,
							db: db_def.database_id,
							tb: Cow::Borrowed(tb),
						};
						if let Some(tb_def) = self.get_key(&table_key, None).await? {
							let cached_tb = Arc::new(TableDefinition::from_stored(&tb_def)?);
							let cached_entry = cache::tx::Entry::Any(
								Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>
							);
							self.cache.insert(qey, cached_entry);
							return Ok(cached_tb);
						}

						if db_def.strict {
							return Err(CatalogError::TbNotFound {
								name: tb.to_owned(),
							}
							.into());
						}

						let stored = StoredTableDefinition::new(
							db_def.namespace_id,
							db_def.database_id,
							self.get_next_tb_id(ctx, db_def.namespace_id, db_def.database_id)
								.await?,
							tb.clone(),
						);
						let tb_def = TableDefinition::from_stored(&stored)?;
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
				let key = TableKey {
					ns: db.namespace_id,
					db: db.database_id,
					tb: Cow::Borrowed(tb),
				};
				let Some(tb) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				return Ok(Some(Arc::new(TableDefinition::from_stored(&tb)?)));
			}
			let qey = cache::tx::Lookup::TbByName(ns, db, tb.as_str());
			match self.cache.get(&qey) {
				Some(val) => val.try_into_type().map(Some),
				None => {
					let Some(db) = self.get_db_by_name(ns, db, None).await? else {
						return Ok(None);
					};

					let key = TableKey {
						ns: db.namespace_id,
						db: db.database_id,
						tb: Cow::Borrowed(tb),
					};
					let Some(tb) = self.get_key(&key, None).await? else {
						return Ok(None);
					};

					let tb = Arc::new(TableDefinition::from_stored(&tb)?);
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
			let stored = tb.to_stored();
			let tb_name = tb.name.clone();
			let key = TableKey {
				ns: tb.namespace_id,
				db: tb.database_id,
				tb: Cow::Borrowed(&tb_name),
			};
			match self.set_key(&key, &stored).await {
				Ok(_) => {}
				Err(e) => {
					if matches!(storage_error(&e), Some(surrealdb_kvs::Error::TransactionReadonly))
					{
						return Err(CatalogError::TbNotFound {
							name: tb_name,
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

			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, tb.name.as_str());
			self.cache.insert(qey, cached_entry.clone());

			let qey = cache::tx::Lookup::TbByName(ns, db, tb.name.as_str());
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
				return Err(CatalogError::TbNotFound {
					name: tb.clone(),
				}
				.into());
			};

			let tb_name = tb.name.clone();
			let key = TableKey {
				ns: tb.namespace_id,
				db: tb.database_id,
				tb: Cow::Borrowed(&tb_name),
			};
			self.del_key(&key).await?;

			// Invalidate the cached list of all tables for this database
			let list_key = cache::tx::Lookup::Tbs(tb.namespace_id, tb.database_id);
			self.cache.remove(&list_key);

			// Clear the cache
			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, tb.name.as_str());
			self.cache.remove(&qey);
			let qey = cache::tx::Lookup::TbByName(ns, db, tb.name.as_str());
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
				return Err(CatalogError::TbNotFound {
					name: tb.clone(),
				}
				.into());
			};

			let tb_name = tb.name.clone();
			let key = TableKey {
				ns: tb.namespace_id,
				db: tb.database_id,
				tb: Cow::Borrowed(&tb_name),
			};
			self.clr_key(&key).await?;

			// Invalidate the cached list of all tables for this database
			let list_key = cache::tx::Lookup::Tbs(tb.namespace_id, tb.database_id);
			self.cache.remove(&list_key);

			// Clear the cache
			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, tb.name.as_str());
			self.cache.remove(&qey);
			let qey = cache::tx::Lookup::TbByName(ns, db, tb.name.as_str());
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
					let range = EventPrefix {
						ns,
						db,
						tb: Cow::Borrowed(tb),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredEventDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Evs(ns, db, tb.as_str());
				match self.cache.get(&qey) {
					Some(val) => val.try_into_evs(),
					None => {
						let range = EventPrefix {
							ns,
							db,
							tb: Cow::Borrowed(tb),
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredEventDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = FieldPrefix {
						ns,
						db,
						tb: Cow::Borrowed(tb),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredFieldDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Fds(ns, db, tb.as_str());
				match self.cache.get(&qey) {
					Some(val) => val.try_into_fds(),
					None => {
						let range = FieldPrefix {
							ns,
							db,
							tb: Cow::Borrowed(tb),
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredFieldDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = IndexDefPrefix {
						ns,
						db,
						tb: Cow::Borrowed(tb),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredIndexDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Ixs(ns, db, tb.as_str());
				match self.cache.get(&qey) {
					Some(val) => val.try_into_ixs(),
					None => {
						let range = IndexDefPrefix {
							ns,
							db,
							tb: Cow::Borrowed(tb),
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredIndexDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = SubscriptionPrefix {
						ns,
						db,
						tb: Cow::Borrowed(tb),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredSubscriptionDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return Ok(compile_subscriptions(tb, &stored));
				}
				let qey = cache::tx::Lookup::Lvs(ns, db, tb.as_str());
				match self.cache.get(&qey) {
					Some(val) => val.try_into_lvs(),
					None => {
						let range = SubscriptionPrefix {
							ns,
							db,
							tb: Cow::Borrowed(tb),
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredSubscriptionDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = compile_subscriptions(tb, &stored);
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
					let key = TableKey {
						ns,
						db,
						tb: Cow::Borrowed(tb),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(TableDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Tb(ns, db, tb.as_str());
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = TableKey {
							ns,
							db,
							tb: Cow::Borrowed(tb),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(TableDefinition::from_stored(&val)?);
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
					let key = EventKey {
						ns,
						db,
						tb: Cow::Borrowed(tb),
						ev: Cow::Borrowed(ev),
					};
					let val = self.get_key(&key, version).await?.ok_or_else(|| {
						CatalogError::EvNotFound {
							name: ev.to_owned(),
						}
					})?;
					return Ok(Arc::new(catalog::EventDefinition::from_stored(&val)?));
				}
				let qey = cache::tx::Lookup::Ev(ns, db, tb.as_str(), ev);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type(),
					None => {
						let key = EventKey {
							ns,
							db,
							tb: Cow::Borrowed(tb),
							ev: Cow::Borrowed(ev),
						};
						let val = self.get_key(&key, None).await?.ok_or_else(|| {
							CatalogError::EvNotFound {
								name: ev.to_owned(),
							}
						})?;
						let val = Arc::new(catalog::EventDefinition::from_stored(&val)?);
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
					let key = FieldKey {
						ns,
						db,
						tb: Cow::Borrowed(tb),
						fd: Cow::Borrowed(fd),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(catalog::FieldDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Fd(ns, db, tb.as_str(), fd);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = FieldKey {
							ns,
							db,
							tb: Cow::Borrowed(tb),
							fd: Cow::Borrowed(fd),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(catalog::FieldDefinition::from_stored(&val)?);
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
			let stored = fd.to_stored();
			let name = fd.name.to_raw_string();
			let key = FieldKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				fd: Cow::Borrowed(&name),
			};
			self.set_key(&key, &stored).await?;

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
			let qey = cache::tx::Lookup::Fd(ns, db, tb.as_str(), &name);
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
					let key = IndexDefKey {
						ns,
						db,
						tb: Cow::Borrowed(tb),
						ix: Cow::Borrowed(ix),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(catalog::IndexDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Ix(ns, db, tb.as_str(), ix);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = IndexDefKey {
							ns,
							db,
							tb: Cow::Borrowed(tb),
							ix: Cow::Borrowed(ix),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(catalog::IndexDefinition::from_stored(&val)?);
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
			let key = IndexNameKey {
				ns,
				db,
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
			let stored = ix.to_stored();
			let key = IndexDefKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: Cow::Borrowed(&ix.name),
			};
			self.set_key(&key, &stored).await?;

			let name_lookup_key = IndexNameKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: ix.index_id,
			};
			self.set_key(&name_lookup_key, &ix.name.to_string()).await?;

			// Invalidate the cached list of all indexes for this table
			let list_key = cache::tx::Lookup::Ixs(ns, db, tb.as_ref());
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Ix(ns, db, tb.as_str(), &ix.name);
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
			let key = IdxRoot {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: ix.index_id,
			};
			self.del_prefix_key(&key).await?;

			// Delete the definition
			let key = IndexDefKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: Cow::Borrowed(&ix.name),
			};
			self.del_key(&key).await?;

			// Delete the id-to-name lookup
			let name_lookup_key = IndexNameKey {
				ns,
				db,
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
					let key = RecordKey {
						ns,
						db,
						tb: Cow::Borrowed(tb),
						id: Cow::Borrowed(id),
					};
					match self.get_key(&key, version).await? {
						Some(record) => Ok(record.into_read_only()),
						None => match self.lightweight_record(ns, db, tb, id, version).await? {
							Some(record) => Ok(record.into_read_only()),
							None => Ok(Arc::new(Default::default())),
						},
					}
				} else {
					let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
					match self.cache.get(&qey) {
						// The entry is in the cache
						Some(val) => val.try_into_record(),
						// The entry is not in the cache
						None => {
							let key = RecordKey {
								ns,
								db,
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
								// A lightweight edge has no record; synthesize
								// it from the adjacency. Cacheable: the delete
								// pipeline clears the entry through
								// `del_record` exactly as for a stored record.
								None => {
									match self.lightweight_record(ns, db, tb, id, None).await? {
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
					let keys: Vec<RecordKey<'_>> = rids
						.iter()
						.map(|rid| RecordKey {
							ns,
							db,
							tb: Cow::Borrowed(&rid.table),
							id: Cow::Borrowed(&rid.key),
						})
						.collect();
					let values = self.get_many_key(keys, version).await?;
					let mut out: Vec<Option<Arc<Record>>> =
						values.into_iter().map(|opt| opt.map(Record::into_read_only)).collect();
					// A miss can be a lightweight edge; resolve every miss in
					// one batched adjacency read.
					let misses: Vec<(usize, &RecordId)> = out
						.iter()
						.enumerate()
						.filter(|(_, o)| o.is_none())
						.map(|(i, _)| (i, &rids[i]))
						.collect();
					if !misses.is_empty() {
						let miss_rids: Vec<&RecordId> =
							misses.iter().map(|(_, rid)| *rid).collect();
						let records = self.lightweight_records(ns, db, &miss_rids, version).await?;
						for ((i, _), record) in misses.into_iter().zip(records) {
							out[i] = Some(match record {
								Some(record) => record.into_read_only(),
								None => Arc::new(Default::default()),
							});
						}
					}
					return out
						.into_iter()
						.map(|o| {
							o.ok_or_else(|| {
								EngineError::Internal("missing record in multi-get batch".into())
									.into()
							})
						})
						.collect();
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
					let keys: Vec<RecordKey<'_>> = uncached_rids
						.iter()
						.map(|(_, rid)| RecordKey {
							ns,
							db,
							tb: Cow::Borrowed(&rid.table),
							id: Cow::Borrowed(&rid.key),
						})
						.collect();
					let values = self.get_many_key(keys, None).await?;
					// Phase 3: populate cache + merge into output.
					//
					// Only populate the cache when the caller requests
					// ReadWrite; ReadOnly avoids eviction churn during
					// large sequential scans. Default (missing) records
					// are never cached, matching `get_record`.
					let place = |out: &mut Vec<Option<Arc<Record>>>,
					             i: usize,
					             rid: &RecordId,
					             record: Arc<Record>| {
						if (record.metadata.is_some() || !record.data.is_none())
							&& matches!(cache_policy, CachePolicy::ReadWrite)
						{
							let qey =
								cache::tx::Lookup::Record(ns, db, rid.table.as_str(), &rid.key);
							let entry = cache::tx::Entry::Val(Arc::clone(&record));
							self.cache.insert(qey, entry);
						}
						out[i] = Some(record);
					};
					// A miss can be a lightweight edge; collect every miss and
					// resolve them in one batched adjacency read.
					let mut misses: Vec<(usize, &RecordId)> = Vec::new();
					for ((i, rid), opt) in uncached_rids.into_iter().zip(values) {
						match opt {
							Some(record) => place(&mut out, i, rid, record.into_read_only()),
							None => misses.push((i, rid)),
						}
					}
					if !misses.is_empty() {
						let miss_rids: Vec<&RecordId> =
							misses.iter().map(|(_, rid)| *rid).collect();
						let records = self.lightweight_records(ns, db, &miss_rids, None).await?;
						for ((i, rid), record) in misses.into_iter().zip(records) {
							let record = match record {
								Some(record) => record.into_read_only(),
								None => Arc::new(Default::default()),
							};
							place(&mut out, i, rid, record);
						}
					}
				}
				// Every slot should be populated by now
				out.into_iter()
					.map(|o| {
						o.ok_or_else(|| {
							EngineError::Internal("missing record in multi-get batch".into()).into()
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
			let key = RecordKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				id: Cow::Borrowed(id),
			};
			if self.exists_key(&key, version).await? {
				return Ok(true);
			}
			// A lightweight edge has no record; its existence is the
			// adjacency's.
			Ok(self.lightweight_record(ns, db, tb, id, version).await?.is_some())
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
				let key = RecordKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				self.put_key(&key, record.as_ref()).await?;
				// Set the value in the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
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
				let key = RecordKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				self.set_key(&key, record.as_ref()).await?;
				// Clear the value from the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
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
				let key = RecordKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				self.del_key(&key).await?;
				// Clear the value from the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
				self.cache.remove(&qey);
				// Return nothing
				Ok(())
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "del_record")),
		)
	}

	fn get_next_tb_id<'a>(
		&'a self,
		ctx: Option<&'a dyn CancellationProbe>,
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
					let range = RootUserPrefix {}.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Rus;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_rus(),
					None => {
						let range = RootUserPrefix {}.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let range = NsUserPrefix {
						ns,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Nus(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nus(),
					None => {
						let range = NsUserPrefix {
							ns,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let range = DbUserPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dus(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dus(),
					None => {
						let range = DbUserPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let key = RootUserKey {
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
						let key = RootUserKey {
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
					let key = NsUserKey {
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
						let key = NsUserKey {
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
					let key = DbUserKey {
						ns,
						db,
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
						let key = DbUserKey {
							ns,
							db,
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
			let key = RootUserKey {
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
			let key = NsUserKey {
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
			let key = DbUserKey {
				ns,
				db,
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
					let range = RootAccessMethodPrefix {}.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredAccessDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Ras;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_ras(),
					None => {
						let range = RootAccessMethodPrefix {}.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredAccessDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = RootGrantPrefix {
						ac: Cow::Borrowed(ra),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Rgs(ra);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_rag(),
					None => {
						let range = RootGrantPrefix {
							ac: Cow::Borrowed(ra),
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let range = NsAccessMethodPrefix {
						ns,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredAccessDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Nas(ns);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nas(),
					None => {
						let range = NsAccessMethodPrefix {
							ns,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredAccessDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = NsGrantPrefix {
						ns,
						ac: Cow::Borrowed(na),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Ngs(ns, na);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nag(),
					None => {
						let range = NsGrantPrefix {
							ns,
							ac: Cow::Borrowed(na),
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let range = DbAccessMethodPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredAccessDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Das(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_das(),
					None => {
						let range = DbAccessMethodPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredAccessDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let range = DbGrantPrefix {
						ns,
						db,
						ac: Cow::Borrowed(da),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					return util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()));
				}
				let qey = cache::tx::Lookup::Dgs(ns, db, da);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_dag(),
					None => {
						let range = DbGrantPrefix {
							ns,
							db,
							ac: Cow::Borrowed(da),
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
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
					let key = RootAccessMethodKey {
						ac: Cow::Borrowed(ra),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(catalog::AccessDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Ra(ra);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = RootAccessMethodKey {
							ac: Cow::Borrowed(ra),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(catalog::AccessDefinition::from_stored(&val)?);
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
					let key = RootGrantKey {
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
						let key = RootGrantKey {
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
					let key = NsAccessMethodKey {
						ns,
						ac: Cow::Borrowed(na),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(catalog::AccessDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Na(ns, na);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = NsAccessMethodKey {
							ns,
							ac: Cow::Borrowed(na),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(catalog::AccessDefinition::from_stored(&val)?);
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
					let key = NsGrantKey {
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
						let key = NsGrantKey {
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
					let key = DbAccessMethodKey {
						ns,
						db,
						ac: Cow::Borrowed(da),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(catalog::AccessDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Da(ns, db, da);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = DbAccessMethodKey {
							ns,
							db,
							ac: Cow::Borrowed(da),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(catalog::AccessDefinition::from_stored(&val)?);
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
					let key = DbGrantKey {
						ns,
						db,
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
						let key = DbGrantKey {
							ns,
							db,
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
			let key = RootAccessMethodKey {
				ac: Cow::Borrowed(ra),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = RootAccessRoot {
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
			let key = NsAccessMethodKey {
				ns,
				ac: Cow::Borrowed(na),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = NsAccessRoot {
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
			let key = DbAccessMethodKey {
				ns,
				db,
				ac: Cow::Borrowed(da),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = DbAccessRoot {
				ns,
				db,
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
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ApiDefinition]>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let range = ApiPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredApiDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Aps(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val,
					None => {
						let range = ApiPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredApiDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = cache::tx::Entry::Aps(catalog::from_stored_all(&stored)?);
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
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ApiDefinition>>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let key = ApiKey {
						ns,
						db,
						ap: Cow::Borrowed(ap),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(catalog::ApiDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Ap(ns, db, ap);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = ApiKey {
							ns,
							db,
							ap: Cow::Borrowed(ap),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = Arc::new(catalog::ApiDefinition::from_stored(&val)?);
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
			let stored = ap.to_stored();
			let name = ap.path.to_string();
			let key = ApiKey {
				ns,
				db,
				ap: Cow::Borrowed(&name),
			};
			self.set_key(&key, &stored).await?;

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
					let range = BucketPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredBucketDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Bus(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_bus(),
					None => {
						let range = BucketPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredBucketDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = catalog::from_stored_all(&stored)?;
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
					let key = BucketKey {
						ns,
						db,
						bu: Cow::Borrowed(bu),
					};
					let Some(val) = self.get_key(&key, version).await? else {
						return Ok(None);
					};
					return Ok(Some(Arc::new(catalog::BucketDefinition::from_stored(&val)?)));
				}
				let qey = cache::tx::Lookup::Bu(ns, db, bu);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Some),
					None => {
						let key = BucketKey {
							ns,
							db,
							bu: Cow::Borrowed(bu),
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let bucket_def = Arc::new(catalog::BucketDefinition::from_stored(&val)?);
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

/// The record a lightweight edge presents to readers, derived entirely from
/// its table, canonical id, and the two endpoints the id carries.
fn synthesize_lightweight_record(
	tb: &TableName,
	id: &RecordIdKey,
	l: &RecordId,
	r: &RecordId,
) -> Record {
	let rid = RecordId {
		table: tb.clone(),
		key: id.clone(),
	};
	let mut data = surrealdb_expr::val::Object::default();
	data.insert("id".to_owned(), Value::RecordId(rid));
	data.insert("in".to_owned(), Value::RecordId(l.clone()));
	data.insert("out".to_owned(), Value::RecordId(r.clone()));
	Record {
		metadata: Some(Metadata {
			record_type: RecordType::Edge {
				variant: LATEST_EDGE_VARIANT,
			},
			aggregation_stats: Vec::new(),
		}),
		data: Value::Object(data),
	}
}

/// The two endpoints of a lightweight edge's canonical `[in, out]` id, when
/// the id has that shape.
pub fn lightweight_edge_parts(id: &RecordIdKey) -> Option<(&RecordId, &RecordId)> {
	let RecordIdKey::Array(arr) = id else {
		return None;
	};
	match arr.as_slice() {
		[Value::RecordId(l), Value::RecordId(r)] => Some((l, r)),
		_ => None,
	}
}

/// Lightweight-relation record synthesis.
///
/// A lightweight relation stores each edge as its two vertex-side adjacency
/// keys alone — no record. The record providers below synthesize the record
/// on demand from the edge's canonical `[in, out]` id, so every reader of
/// records — point reads, the processors, the delete pipeline, cascades,
/// permission gates, FETCH — is correct without knowing the table is
/// lightweight. Existence is decided by the merged adjacency truth for the
/// `in` endpoint: the exact pointer key (live or tombstone) wins, and a
/// folded vertex table is additionally probed through its covering packed
/// chunk, resolving numeric entries through the shared doc-id space.
impl Transaction {
	/// Synthesizes the record for a lightweight edge, or returns `None`
	/// when the id is not a lightweight edge of this table or the edge does
	/// not exist at the read snapshot.
	async fn lightweight_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Option<Record>> {
		let Some((l, r)) = lightweight_edge_parts(id) else {
			return Ok(None);
		};
		let Some(tbdef) = self.get_tb(ns, db, tb, version).await? else {
			return Ok(None);
		};
		let TableType::Relation(rel) = &tbdef.table_type else {
			return Ok(None);
		};
		if !rel.lightweight {
			return Ok(None);
		}
		if !self.lightweight_edge_present(ns, db, tb, l, r, id, version).await? {
			return Ok(None);
		}
		Ok(Some(synthesize_lightweight_record(tb, id, l, r)))
	}

	/// Batched [`Self::lightweight_record`]: one slot per input id, `None`
	/// where the id is not a live lightweight edge.
	///
	/// The candidates' exact pointer keys are read with a single batched get;
	/// only edges whose `in` vertex table has folded adjacency fall back to
	/// the per-edge covering-chunk probe, and the last fetched chunk is
	/// memoized per `(in vertex, edge table)` scope, so a hub's edges — which
	/// typically share their covering chunk — do not refetch it.
	async fn lightweight_records(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: &[&RecordId],
		version: Option<u64>,
	) -> Result<Vec<Option<Record>>> {
		use surrealdb_expr::expr::Dir;
		let mut out: Vec<Option<Record>> = (0..rids.len()).map(|_| None).collect();
		// Keep the ids with the canonical `[in, out]` shape on a lightweight
		// relation; anything else can never synthesize a record.
		let mut candidates: Vec<(usize, &RecordId, &RecordId, &RecordId)> = Vec::new();
		for (i, rid) in rids.iter().enumerate() {
			let Some((l, r)) = lightweight_edge_parts(&rid.key) else {
				continue;
			};
			let Some(tbdef) = self.get_tb(ns, db, &rid.table, version).await? else {
				continue;
			};
			let TableType::Relation(rel) = &tbdef.table_type else {
				continue;
			};
			if !rel.lightweight {
				continue;
			}
			candidates.push((i, rid, l, r));
		}
		if candidates.is_empty() {
			return Ok(out);
		}
		// One batched read over the candidates' exact `in`-side pointer keys:
		// a live value proves the edge, a tombstone proves its absence.
		let keys: Vec<GraphPointerKey<'_>> = candidates
			.iter()
			.map(|(_, rid, l, r)| GraphPointerKey {
				ns,
				db,
				tb: Cow::Borrowed(&l.table),
				id: Cow::Borrowed(&l.key),
				dir: Dir::Out,
				foreign_table: Cow::Borrowed(&rid.table),
				foreign_key: Cow::Borrowed(&rid.key),
				target_table: Cow::Borrowed(&r.table),
				target_key: Cow::Borrowed(&r.key),
			})
			.collect();
		let values = self.get_many_key(keys, version).await?;
		// A pointer-key miss can only live in a packed chunk, which only
		// exists once the `in` vertex's table has folded. The covering-chunk
		// probe stays per-edge, but the fetched chunk is memoized per scope:
		// an edge found in the memoized block is present (it has no
		// tombstone, or the batched read above would have seen it), and a
		// probe that lands on the memoized chunk again resolves without
		// refetching the block.
		type Scope = (TableName, RecordIdKey, TableName);
		// The scope tuple hashes by value; the ids in it are never mutated
		// while memoized.
		#[expect(clippy::mutable_key_type)]
		let mut chunks: HashMap<Scope, (Vec<u8>, AdjacencyBlock)> = HashMap::new();
		for ((i, rid, l, r), value) in candidates.into_iter().zip(values) {
			if let Some(value) = value {
				if !value.is_tombstone() {
					out[i] = Some(synthesize_lightweight_record(&rid.table, &rid.key, l, r));
				}
				continue;
			}
			let folded =
				self.get_tb(ns, db, &l.table, version).await?.is_some_and(|tb| tb.graph_folded);
			if !folded {
				continue;
			}
			let scope = (l.table.clone(), l.key.clone(), rid.table.clone());
			if let Some((_, block)) = chunks.get(&scope)
				&& self.block_holds_edge(ns, db, &rid.table, block, &rid.key, r, version).await?
			{
				out[i] = Some(synthesize_lightweight_record(&rid.table, &rid.key, l, r));
				continue;
			}
			// The covering chunk: the last chunk starting at or before the
			// edge key; entries are partitioned by `chunk_start`, so no other
			// chunk can hold it.
			let up_to_bound = AdjacencyEdgeTablePrefix {
				ns,
				db,
				tb: Cow::Borrowed(&l.table),
				id: Cow::Borrowed(&l.key),
				dir: Dir::Out,
				edge_table: Cow::Borrowed(&rid.table),
			}
			.range_where((
				std::ops::Bound::Unbounded,
				std::ops::Bound::Included(Cow::Borrowed(&rid.key)),
			))?;
			let covering = self.keysr_raw(up_to_bound, 1, 0, version).await?;
			let Some(chunk_key_bytes) = covering.into_iter().next() else {
				continue;
			};
			// The probe landed on the memoized chunk, which was already
			// checked above: the edge is absent.
			if chunks.get(&scope).is_some_and(|(key, _)| *key == chunk_key_bytes) {
				continue;
			}
			let chunk_key = AdjacencyKey::decode_key(&chunk_key_bytes)?;
			let Some(block) = self.get_key(&chunk_key, version).await? else {
				continue;
			};
			let holds =
				self.block_holds_edge(ns, db, &rid.table, &block, &rid.key, r, version).await?;
			chunks.insert(scope, (chunk_key_bytes, block));
			if holds {
				out[i] = Some(synthesize_lightweight_record(&rid.table, &rid.key, l, r));
			}
		}
		Ok(out)
	}

	/// Whether the lightweight edge `l -[tb:id]-> r` exists at the read
	/// snapshot: its exact `in`-side pointer key (live beats absent,
	/// tombstone beats everything), falling back to the covering packed
	/// chunk when the `in` vertex's table has folded adjacency.
	#[allow(clippy::too_many_arguments)]
	async fn lightweight_edge_present(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		l: &RecordId,
		r: &RecordId,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<bool> {
		use surrealdb_expr::expr::Dir;
		let key = GraphPointerKey {
			ns,
			db,
			tb: Cow::Borrowed(&l.table),
			id: Cow::Borrowed(&l.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(tb),
			foreign_key: Cow::Borrowed(id),
			target_table: Cow::Borrowed(&r.table),
			target_key: Cow::Borrowed(&r.key),
		};
		if let Some(value) = self.get_key(&key, version).await? {
			return Ok(!value.is_tombstone());
		}
		// No delta key: the edge can only live in a packed chunk, which only
		// exists once the vertex table has folded.
		let folded =
			self.get_tb(ns, db, &l.table, version).await?.is_some_and(|tb| tb.graph_folded);
		if !folded {
			return Ok(false);
		}
		// The covering chunk: the last chunk starting at or before the edge
		// key; entries are partitioned by `chunk_start`, so no other chunk
		// can hold it.
		let prefix = || AdjacencyEdgeTablePrefix {
			ns,
			db,
			tb: Cow::Borrowed(&l.table),
			id: Cow::Borrowed(&l.key),
			dir: Dir::Out,
			edge_table: Cow::Borrowed(tb),
		};
		let up_to_bound = prefix().range_where((
			std::ops::Bound::Unbounded,
			std::ops::Bound::Included(Cow::Borrowed(id)),
		))?;
		let covering = self.keysr_raw(up_to_bound, 1, 0, version).await?;
		let Some(chunk_key) = covering.into_iter().next() else {
			return Ok(false);
		};
		let chunk_key = AdjacencyKey::decode_key(&chunk_key)?;
		let Some(block) = self.get_key(&chunk_key, version).await? else {
			return Ok(false);
		};
		self.block_holds_edge(ns, db, tb, &block, id, r, version).await
	}

	/// Resolves `id` to the doc-ID this table has mapped it to, or `None`.
	///
	/// The forward mapping is spelled two ways. `!dj` keys the record id through
	/// a lossless encoding and is injective. `!di` keys it through `IndexFormat`,
	/// which erases a number's kind, so every id with the same decimal expansion
	/// shares one key — `c:[1]` and `c:[1dec]` land on the same one.
	///
	/// A hit on either is therefore adopted only when the reverse mapping `!dd`
	/// agrees that the doc-ID names the record asked for. `!dd` stores the record
	/// key as a value and keeps it exactly, so it is what decides. Reading `!di`
	/// without that check resolves one of a colliding pair to the other's doc-ID.
	///
	/// `version` selects the snapshot to resolve against: mappings are removed
	/// when their record is deleted, so a time-travel read must resolve through
	/// the state its snapshot saw.
	pub async fn resolve_doc_id_verified(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Option<DocId>> {
		Ok(self.resolve_doc_id_mapping(ns, db, tb, id, version).await?.map(|(doc_id, _)| doc_id))
	}

	/// [`Self::resolve_doc_id_verified`], and which direction answered.
	///
	/// `true` means the injective key did; `false` means only the type-erased
	/// one held the mapping, which is what a record mapped by a node on the
	/// previous release looks like. A caller in a position to write can publish
	/// the injective mapping for such a record, and one that only reads can
	/// ignore the flag.
	pub async fn resolve_doc_id_mapping(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Option<(DocId, bool)>> {
		let names_this_record = async |doc_id: DocId| -> Result<bool> {
			let dd = DocKeyKey::new(ns, db, Cow::Borrowed(tb), doc_id);
			let Some(mapped) = self.get_key(&dd, version).await? else {
				return Ok(false);
			};
			Ok(mapped.addresses_same_record(id))
		};

		let dj = DocLookupIdentityKey::new(ns, db, Cow::Borrowed(tb), RecordIdentity(id.clone()));
		if let Some(doc_id) = self.get_key(&dj, version).await?
			&& names_this_record(doc_id).await?
		{
			return Ok(Some((doc_id, true)));
		}
		let di = DocLookupKey::new(ns, db, Cow::Borrowed(tb), Cow::Borrowed(id));
		let Some(doc_id) = self.get_key(&di, version).await? else {
			return Ok(None);
		};
		Ok(names_this_record(doc_id).await?.then_some((doc_id, false)))
	}

	/// Whether a decoded packed chunk holds the edge `(id -> r)`, matching
	/// on the full entry identity. Numeric chunks are matched through the
	/// verified forward key→doc-id mapping; a key with no mapping was never
	/// compressed and cannot be in a numeric chunk.
	#[allow(clippy::too_many_arguments)]
	async fn block_holds_edge(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		block: &AdjacencyBlock,
		id: &RecordIdKey,
		r: &RecordId,
		version: Option<u64>,
	) -> Result<bool> {
		match &block.entries {
			BlockEntries::Plain(entries) => {
				let edge_key = storekey::encode_vec(id).map_err(anyhow::Error::from_boxed)?;
				let target_key = storekey::encode_vec(&r.key).map_err(anyhow::Error::from_boxed)?;
				Ok(entries.iter().any(|e| {
					e.edge_key == edge_key
						&& e.target_key == target_key
						&& block.targets.get(e.target_table as usize).is_some_and(|t| *t == r.table)
				}))
			}
			BlockEntries::Numeric(entries) => {
				// Both sides resolve through the verified injective mapping: a
				// raw `!di` read would answer one of a numerically colliding
				// pair with the other's doc-ID, reporting the edge absent or
				// matching the wrong one.
				let Some(edge_id) = self.resolve_doc_id_verified(ns, db, tb, id, version).await?
				else {
					return Ok(false);
				};
				let Some(entry) = entries.iter().find(|e| e.edge == edge_id) else {
					return Ok(false);
				};
				if block.targets.get(entry.target_table as usize) != Some(&r.table) {
					return Ok(false);
				}
				let target =
					self.resolve_doc_id_verified(ns, db, &r.table, &r.key, version).await?;
				Ok(target == Some(entry.target))
			}
		}
	}
}

#[cfg(test)]
mod compare_swap_tests {
	use std::sync::Arc;

	use anyhow::Result;
	use revision::revisioned;
	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::key::KVKey;
	use surrealdb_kvs::value::KVValue;
	use surrealdb_kvs::{Key, TransactionType};

	use crate::factory::TransactionFactory;
	use crate::sequences::Sequences;

	/// A stored value as an earlier release wrote it: one field, revision 1.
	#[revisioned(revision = 1)]
	#[derive(Clone, Debug, PartialEq)]
	struct StateV1 {
		generation: u64,
	}

	/// The same value after a field was added: revision 2. Decoding the earlier
	/// revision fills the new field with its default, which is what makes the
	/// two spellings mean the same thing while encoding differently.
	#[revisioned(revision = 2)]
	#[derive(Clone, Debug, PartialEq)]
	struct StateV2 {
		generation: u64,
		#[revision(start = 2)]
		cursor: Option<u64>,
	}

	surrealdb_kvs::impl_kv_value_revisioned!(StateV1);
	surrealdb_kvs::impl_kv_value_revisioned!(StateV2);

	#[derive(Debug)]
	struct StateKey;

	impl KVKey for StateKey {
		type Value = StateV2;

		fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
			buffer.extend_from_slice(b"/!cas-revision-test");
			Ok(())
		}

		fn value_context(&self) {}
	}

	async fn factory() -> (TransactionFactory, Sequences) {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf = TransactionFactory::new(
			Arc::new(crate::triggers::CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let sequences = Sequences::new(tf.clone(), uuid::Uuid::new_v4());
		(tf, sequences)
	}

	/// A compare-and-swap must fence on what is stored, not on a re-encoding of
	/// it.
	///
	/// A revisioned value encodes at whatever revision the running build
	/// declares, which need not be the one it was stored at. Encoding the check
	/// value here and comparing those bytes therefore never matches a value an
	/// earlier release wrote, so the swap cannot succeed however often it is
	/// retried — and a caller that retries a failed acquisition spins instead of
	/// making progress.
	#[tokio::test]
	async fn a_swap_matches_a_value_stored_at_an_earlier_revision() {
		let (tf, sq) = factory().await;
		let key = Key::from(b"/!cas-revision-test".to_vec());

		// Exactly what the earlier release left behind.
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		tx.put_compare(
			key.clone(),
			StateV1 {
				generation: 7,
			}
			.kv_encode_value()
			.unwrap(),
			None,
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();

		// This build reads it through the current revision ...
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let current = tx.get_key(&StateKey, None).await.unwrap().expect("the stored state");
		assert_eq!(
			current,
			StateV2 {
				generation: 7,
				cursor: None
			}
		);

		// ... and swaps it for the next one, fencing on what it just read.
		let next = StateV2 {
			generation: 8,
			cursor: None,
		};
		tx.put_compare_key(&StateKey, &next, Some(&current)).await.unwrap();
		tx.commit().await.unwrap();

		let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
		assert_eq!(tx.get_key(&StateKey, None).await.unwrap(), Some(next));
		tx.cancel().await.unwrap();
	}

	/// Fencing on the stored bytes must not make every swap succeed: a check
	/// value that disagrees with what is stored still fails, and a swap against
	/// a key holding nothing fails too.
	#[tokio::test]
	async fn a_swap_still_fails_when_the_check_value_disagrees() {
		let (tf, sq) = factory().await;

		// Nothing stored: a swap expecting a value must not create one.
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let absent = StateV2 {
			generation: 1,
			cursor: None,
		};
		assert!(tx.put_compare_key(&StateKey, &absent, Some(&absent)).await.is_err());
		tx.cancel().await.unwrap();

		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		tx.put_compare_key(
			&StateKey,
			&StateV2 {
				generation: 7,
				cursor: None,
			},
			None,
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();

		// Stored, but the check value names a different state.
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let wrong = StateV2 {
			generation: 99,
			cursor: None,
		};
		let next = StateV2 {
			generation: 8,
			cursor: None,
		};
		assert!(tx.put_compare_key(&StateKey, &next, Some(&wrong)).await.is_err());
		tx.cancel().await.unwrap();

		// A field that only the current revision carries still distinguishes.
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let cursor_set = StateV2 {
			generation: 7,
			cursor: Some(3),
		};
		assert!(tx.put_compare_key(&StateKey, &next, Some(&cursor_set)).await.is_err());
		tx.cancel().await.unwrap();

		// The value is untouched by any of the failed swaps.
		let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
		assert_eq!(
			tx.get_key(&StateKey, None).await.unwrap(),
			Some(StateV2 {
				generation: 7,
				cursor: None
			})
		);
		tx.cancel().await.unwrap();
	}

	/// A stored value that will not decode must fail the *comparison*, not the
	/// statement.
	///
	/// Callers treat the refusal as the benign outcome and anything else as
	/// fatal, and several exist precisely to tolerate a value an older or newer
	/// binary wrote — a delete that tolerates a foreign index entry, a session
	/// sweep that skips what it cannot read. Surfacing a decode error here would
	/// escape into those callers as a failed statement.
	#[tokio::test]
	async fn an_undecodable_stored_value_fails_the_comparison_not_the_statement() {
		let (tf, sq) = factory().await;
		let key = Key::from(b"/!cas-revision-test".to_vec());
		let chk = StateV2 {
			generation: 7,
			cursor: None,
		};

		for which in ["put", "del", "clr"] {
			// Bytes no revisioned decode will accept.
			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			tx.set(key.clone(), b"not a revisioned value".to_vec()).await.unwrap();
			tx.commit().await.unwrap();

			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			let e = match which {
				"put" => tx.put_compare_key(&StateKey, &chk, Some(&chk)).await,
				"del" => tx.del_compare_key(&StateKey, Some(&chk)).await,
				_ => tx.clr_compare_key(&StateKey, Some(&chk)).await,
			}
			.expect_err("an undecodable value cannot satisfy the check");
			assert!(
				matches!(
					crate::storage_error(&e),
					Some(surrealdb_kvs::Error::TransactionConditionNotMet)
				),
				"{which}: expected the comparison to be refused, got: {e}"
			);
			tx.cancel().await.unwrap();

			// And the bytes are still there, untouched by the refusal.
			let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
			assert_eq!(
				tx.get(key.clone(), None).await.unwrap().as_deref(),
				Some(b"not a revisioned value".as_slice()),
				"{which}: the stored value must survive"
			);
			tx.cancel().await.unwrap();
		}
	}

	/// The conditional deletes fence the same way, and each is worth covering
	/// because each reaches the backend through a different method.
	///
	/// A delete that cannot match a value an earlier release wrote is the same
	/// defect wearing different clothes: the caller sees a mismatch where the
	/// value has not changed, and a retrying one never finishes.
	#[tokio::test]
	async fn a_conditional_delete_matches_a_value_stored_at_an_earlier_revision() {
		let (tf, sq) = factory().await;
		let key = Key::from(b"/!cas-revision-test".to_vec());

		let stored = StateV2 {
			generation: 7,
			cursor: None,
		};
		let wrong = StateV2 {
			generation: 99,
			cursor: None,
		};

		for which in ["del", "clr"] {
			// Exactly what the earlier release left behind.
			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			tx.put_compare(
				key.clone(),
				StateV1 {
					generation: 7,
				}
				.kv_encode_value()
				.unwrap(),
				None,
			)
			.await
			.unwrap();
			tx.commit().await.unwrap();

			// A check value that disagrees still fails, and leaves the value.
			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			let refused = match which {
				"del" => tx.del_compare_key(&StateKey, Some(&wrong)).await,
				_ => tx.clr_compare_key(&StateKey, Some(&wrong)).await,
			};
			assert!(refused.is_err(), "{which}: a disagreeing check value must not delete");
			tx.cancel().await.unwrap();

			let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
			assert_eq!(tx.get_key(&StateKey, None).await.unwrap().as_ref(), Some(&stored));
			tx.cancel().await.unwrap();

			// The value this build reads back does match, across the revisions.
			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			match which {
				"del" => tx.del_compare_key(&StateKey, Some(&stored)).await,
				_ => tx.clr_compare_key(&StateKey, Some(&stored)).await,
			}
			.unwrap_or_else(|e| panic!("{which}: {e}"));
			tx.commit().await.unwrap();

			let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
			assert_eq!(tx.get_key(&StateKey, None).await.unwrap(), None, "{which}: not deleted");
			tx.cancel().await.unwrap();
		}
	}
}
