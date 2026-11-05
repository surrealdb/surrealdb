//! The materialiser: the per-index passes that fold a queued kind's
//! write-side backlog into the read-optimised structure its queries use.
//!
//! A queued kind is one whose writes leave work behind them
//! ([`Index::queues_compaction`]): full-text term and statistics deltas, count
//! deltas, and HNSW and DiskANN pendings. Every kind merges its backlog at read
//! time, so a pass changes how much work a read does, never what it answers.
//!
//! These passes are the only code that folds a queue. Two callers run them:
//! background compaction, which drains the `!ic` request queue under a
//! cluster-wide lease ([`Datastore::index_compaction`]), and a blocking
//! `DEFINE INDEX` or `REBUILD INDEX` waiting, once its transaction has
//! committed, for its own index to be materialised
//! ([`MaterialiseAfterCommit`]). A [`Materialiser`] is built from a context,
//! so a statement runs a pass without a [`Datastore`].
//!
//! Two passes on one index are safe whoever runs them: a plan records the
//! compaction generation it read, and its apply advances that generation with
//! a conditional write, so of two plans read from one state at most one
//! applies. A process-local mutex per index ([`IndexBuilder::materialise_lock`])
//! keeps two passes in one process from planning the same work; it saves
//! work, and correctness does not rest on it.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use common::time::sleep;
use surrealdb_datastore::close::{AfterCommitDeadline, AfterCommitWait};
use surrealdb_observe::{
	ExecutionObserver, IndexCompactionKind, IndexCompactionPassEvent, IndexCompactionPassEventCtx,
	IndexCompactionPassEventSafe, IndexCompactionPassOutcome, IndexCompactionPlanEvent,
	IndexCompactionPlanEventCtx, IndexCompactionPlanEventSafe, IndexCompactionPlanMode,
	IndexCompactionRetryEvent, IndexCompactionRetryEventCtx, IndexCompactionRetryEventSafe,
	IndexCompactionRetryReason, IndexCompactionTrigger, IndexHnswCacheDiscardEvent,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use web_time::Instant;

use super::IndexBuilder;
use super::state::{
	BuildOwner, build_basis_is_stale, build_owner, build_owner_of, clear_lapsed_online_owner,
	lapsed_online_owners_clear, retire_uncommitted_build, uncommitted_build_lapsed,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::{FromStored, Index, IndexDefinition};
use crate::ctx::reason::Reason;
use crate::ctx::{Context, FrozenContext};
use crate::err::EngineError;
use crate::idx::index::IndexOperation;
use crate::idx::trees::hnsw::index::HnswCompactionOutcome;
use crate::idx::{CompactionApply, IndexKeyBase};
use crate::key::schema::{IndexDefKey, IndexNameKey};
use crate::kvs::TransactionType::{Read, Write};
use crate::kvs::sequences::Sequences;
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::kvs::{
	Datastore, DatastoreError, Transaction, TransactionType, is_retryable_transaction_conflict,
};

/// What a pass reads at its plan boundaries to learn that it should stop: the
/// cluster-wide lease background compaction runs under, found held by another
/// node. A pass that stops this way reports [`CompactionPassOutcome::Budget`]
/// and requeues its index, so the lease's holder compacts the rest.
pub(crate) trait PassLease: Send + Sync {
	/// Whether the lease has been found lost.
	fn is_lost(&self) -> bool;
}

const TARGET: &str = "surrealdb::core::kvs::index::materialise";

/// Most compaction plans one pass over an index applies.
///
/// A pass that spends it with work left requeues its index and ends on
/// [`CompactionPassOutcome::Budget`], so the queue batch that named the index,
/// and every other index that batch names, waits on at most this many of its
/// plans rather than on its whole backlog. The rotation in
/// [`Datastore::index_compaction`] reaches the requeued index again after the
/// indexes queued behind it; where that call ends after a short batch read
/// from the head of the queue, the call the request's commit wakes does. Each
/// plan is bounded by its kind's own caps, so this bounds the work one pass
/// does, not how long it takes.
pub(crate) const INDEX_COMPACTION_PASS_PLAN_BUDGET: usize = 4;

/// Most retries one compaction pass makes, counting both retryable conflicts
/// and plans that lost their race.
///
/// A pass that meets one more requeues its index, and returns the conflict or
/// ends on [`CompactionPassOutcome::LostRace`], so a later pass retries the
/// index rather than this one spinning against it, and the index does not wait
/// for its next write either.
pub(crate) const INDEX_COMPACTION_PASS_MAX_RETRIES: usize = 5;

/// How one compaction pass over an index ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompactionPassOutcome {
	/// Nothing to fold: the first plan found no backlog, or the index has no
	/// definition this pass compacts and no live build owner, so it has been
	/// removed or its defining statement rolled back.
	Empty,
	/// The pass applied at least one plan and then ran out of work: the last
	/// plan it applied reported nothing beyond it, or the next one found
	/// nothing. The backlog the pass read is folded.
	Short,
	/// The pass stopped with work left and requeued its index: it applied
	/// [`INDEX_COMPACTION_PASS_PLAN_BUDGET`] plans, or found its lease lost at a
	/// plan boundary.
	Budget,
	/// A plan was stale when applied, and the pass had no retry left to read a
	/// fresh one: another compactor advanced the compaction generation first,
	/// or an entry the plan captured changed before its write. The pass
	/// requeued its index, since what the stale plan captured may still be
	/// unfolded.
	LostRace,
	/// A live build owner holds the vector index, so the pass left it to that
	/// build. The pass did not act on its requests, so the batch keeps one.
	Fenced,
	/// The index has no definition this pass can read, but a live build owner
	/// holds it: the statement defining it has not committed. The pass did not
	/// act on its requests, so the batch keeps one, which the first pass after
	/// that commit acts on.
	NotVisible,
}

impl CompactionPassOutcome {
	/// Whether the pass could not act on the requests that led to it, so the
	/// batch keeps one of them in place for a later pass.
	pub(crate) fn retains_request(self) -> bool {
		matches!(self, Self::Fenced | Self::NotVisible)
	}

	fn observe_outcome(self) -> IndexCompactionPassOutcome {
		match self {
			Self::Empty => IndexCompactionPassOutcome::Empty,
			Self::Short => IndexCompactionPassOutcome::Short,
			Self::Budget => IndexCompactionPassOutcome::Budget,
			Self::LostRace => IndexCompactionPassOutcome::LostRace,
			Self::Fenced => IndexCompactionPassOutcome::Fenced,
			Self::NotVisible => IndexCompactionPassOutcome::NotVisible,
		}
	}
}

fn compaction_kind(index: &Index) -> Option<IndexCompactionKind> {
	match index {
		Index::FullText(_) => Some(IndexCompactionKind::FullText),
		Index::Count(_) => Some(IndexCompactionKind::Count),
		Index::Hnsw(_) => Some(IndexCompactionKind::Hnsw),
		#[cfg(diskann)]
		Index::DiskAnn(_) => Some(IndexCompactionKind::DiskAnn),
		_ => None,
	}
}

/// How one pass over an index ended, as the queue batch that ran it needs to
/// know.
pub(crate) struct CompactionPassReport {
	/// The pass's outcome, or the error that ended it.
	pub(crate) result: Result<CompactionPassOutcome>,
	/// Whether the pass owed its index a fresh compaction request and could
	/// not queue it. The requests that led to the pass are then the only record
	/// of the index's remaining work.
	pub(crate) unqueued: bool,
}

/// A compaction pass's progress through its plan budget and its retries.
struct CompactionPassProgress {
	kind: IndexCompactionKind,
	observer: Arc<dyn ExecutionObserver>,
	/// Plans the pass has applied.
	applied: usize,
	/// Retryable conflicts and lost races the pass has retried.
	retries: usize,
}

impl CompactionPassProgress {
	fn new(kind: IndexCompactionKind, observer: Arc<dyn ExecutionObserver>) -> Self {
		Self {
			kind,
			observer,
			applied: 0,
			retries: 0,
		}
	}

	/// How the pass ends on a plan with nothing to apply.
	fn nothing_to_apply(&self) -> CompactionPassOutcome {
		if self.applied == 0 {
			CompactionPassOutcome::Empty
		} else {
			CompactionPassOutcome::Short
		}
	}

	/// Counts an applied plan, emits its plan event when it folded at least one
	/// pending key, and says how the pass ends if it ends there. `has_more` is
	/// whether that plan stopped at its own cap with entries behind it. A plan
	/// that folded nothing still counts toward the pass's plan budget. Callers
	/// record a committed plan before they honour cancellation, so the plan
	/// metrics count every fold that reached storage.
	fn record_applied(
		&mut self,
		mode: IndexCompactionPlanMode,
		keys_compacted: usize,
		duration: Duration,
		has_more: bool,
	) -> Option<CompactionPassOutcome> {
		self.applied += 1;
		if keys_compacted > 0 {
			self.observer.on_index_compaction_plan(&IndexCompactionPlanEvent {
				safe: IndexCompactionPlanEventSafe {
					kind: self.kind,
					mode,
					duration,
					pending_entries: keys_compacted as u64,
				},
				ctx: IndexCompactionPlanEventCtx::default(),
			});
		}
		if !has_more {
			Some(CompactionPassOutcome::Short)
		} else if self.applied >= INDEX_COMPACTION_PASS_PLAN_BUDGET {
			Some(CompactionPassOutcome::Budget)
		} else {
			None
		}
	}

	/// Records an in-place HNSW plan that mutated the resident graph and then
	/// did not commit, so the process-local index was discarded.
	fn record_discarded_cache(&self) {
		self.observer.on_index_hnsw_cache_discard(&IndexHnswCacheDiscardEvent::default());
	}

	/// Whether a plan that lost its race is retried from a fresh plan, while
	/// the pass has retries left. A lost race is a plan read from a state
	/// another writer has since moved past, not a sign that nothing is left to
	/// fold, so the pass reads the state again rather than ending.
	fn retry_lost_race(&mut self, ikb: &IndexKeyBase) -> bool {
		if self.retries >= INDEX_COMPACTION_PASS_MAX_RETRIES {
			return false;
		}
		self.retries += 1;
		self.observer.on_index_compaction_retry(&IndexCompactionRetryEvent {
			safe: IndexCompactionRetryEventSafe {
				kind: self.kind,
				reason: IndexCompactionRetryReason::LostRace,
			},
			ctx: IndexCompactionRetryEventCtx::default(),
		});
		debug!(target: TARGET, "Index compaction plan for {ikb} lost its race, replanning");
		true
	}

	/// Whether `err` is retried: a retryable conflict, while the pass has
	/// retries left. Waits before a retry.
	async fn retry(&mut self, err: &anyhow::Error, operation: impl Into<String>) -> bool {
		if self.retries >= INDEX_COMPACTION_PASS_MAX_RETRIES {
			return false;
		}
		if !Datastore::retry_index_operation_conflict(err, operation).await {
			return false;
		}
		self.retries += 1;
		self.observer.on_index_compaction_retry(&IndexCompactionRetryEvent {
			safe: IndexCompactionRetryEventSafe {
				kind: self.kind,
				reason: IndexCompactionRetryReason::TxnConflict,
			},
			ctx: IndexCompactionRetryEventCtx::default(),
		});
		true
	}
}

/// What [`compaction_write_guard`] found in an apply's write transaction.
pub(crate) enum CompactionWriteGuard {
	/// The index is live under this definition and no build fences it, so the
	/// apply may write.
	Live(Arc<IndexDefinition>),
	/// The index has no definition under its id, or is being removed, so
	/// nothing may be written under its prefix.
	Retired,
	/// A live build owner holds the vector index.
	Fenced,
}

/// Whether a live build owner fences compaction off this kind of index: the
/// kinds whose fold resolves doc-IDs and loads the graph through the
/// definition's layout, which only the build that knows a new layout may
/// fold through.
fn fenced_by_build_owner(index: &Index) -> bool {
	match index {
		Index::Hnsw(_) => true,
		#[cfg(diskann)]
		Index::DiskAnn(_) => true,
		_ => false,
	}
}

/// The check every compaction apply makes in its own write transaction, before
/// its first write.
///
/// What a pass read before this transaction began — its dispatch and its plan —
/// was read in transactions that have closed, so it binds nothing here. The
/// index's catalog definition is taken with a locked read, resolved the way
/// [`crate::catalog::providers::TableProvider::get_tb_index_by_id`] resolves it: the id to the
/// name, then the definition stored under that name. A retirement deletes that
/// definition, so one that commits between this read and the transaction's
/// commit fails the commit with a retryable conflict, rather than letting the
/// apply land index data under a prefix the catalog no longer names. An index
/// [`fenced_by_build_owner`] also takes a locked read of its build state, so a
/// build that acquires it after this read conflicts the same way. An `Online`
/// owner whose heartbeat lapsed is cleared here, in the apply's transaction,
/// where the store allows it ([`clear_lapsed_online_owner`]): the apply then
/// commits only if the statement that owner's build waited on can no longer.
///
/// The reads bind only the transaction that registers them, so `tx` must be
/// the one the apply commits.
pub(crate) async fn compaction_write_guard(
	tx: &Transaction,
	ikb: &IndexKeyBase,
) -> Result<CompactionWriteGuard> {
	let name = IndexNameKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: ikb.index(),
	};
	let Some(name) = tx.get_key(&name, None).await? else {
		return Ok(CompactionWriteGuard::Retired);
	};
	let definition = IndexDefKey {
		ns: ikb.ns(),
		db: ikb.db(),
		tb: Cow::Borrowed(ikb.table()),
		ix: Cow::Borrowed(name.as_str()),
	};
	let Some(stored) = tx.getu_key(&definition).await? else {
		return Ok(CompactionWriteGuard::Retired);
	};
	let ix = IndexDefinition::from_stored(&stored)?;
	if ix.index_id != ikb.index() || ix.prepare_remove {
		return Ok(CompactionWriteGuard::Retired);
	}
	if fenced_by_build_owner(&ix.index) {
		match build_owner(tx, ikb, true).await? {
			BuildOwner::Held => return Ok(CompactionWriteGuard::Fenced),
			// A lapsed generation built against what the catalog does not
			// describe is not folded through the definition the catalog holds:
			// the pass withdraws it in a transaction of its own
			// ([`Materialiser::withdraw_stale_build`]), and this apply is
			// fenced meanwhile.
			BuildOwner::Lapsed(state) => {
				if build_basis_is_stale(tx, ikb, &state).await? {
					return Ok(CompactionWriteGuard::Fenced);
				}
				clear_lapsed_online_owner(tx, ikb, &state).await?;
			}
			BuildOwner::Released => {}
		}
	}
	Ok(CompactionWriteGuard::Live(Arc::new(ix)))
}

#[cfg(diskann)]
/// How one DiskANN frame of a pass ended, short of an error.
enum DiskAnnFrameStep {
	/// The plan applied and committed; `has_more` says it stopped at a cap.
	Applied {
		has_more: bool,
		keys_compacted: usize,
		duration: Duration,
	},
	/// Nothing to apply: no backlog, or the index is gone or not DiskANN.
	Empty,
	/// A live build owner holds the index.
	Fenced,
	/// The plan was stale when applied.
	LostRace,
}

#[cfg(diskann)]
/// Where one DiskANN frame of a pass failed, which says how the pass retries.
enum DiskAnnFrameError {
	/// Reading the plan or opening a transaction: returned as is.
	Plan(anyhow::Error),
	/// The write guard: a retryable conflict is retried.
	Guard(anyhow::Error),
	/// The apply or its commit: a retryable conflict is retried.
	Apply(anyhow::Error),
}

#[cfg(diskann)]
/// The environment a pass applies a plan in: the pass's context, which answers
/// to no deadline or cancellation of its own. With an `abort` token it reports
/// done once that is tripped, and the context's own checks, such as the memory
/// threshold, otherwise, so a frame in progress stops at its next check rather
/// than running to its end. Without one it is never done.
struct PassEnv<'a> {
	inner: &'a dyn crate::idx::env::IndexEnv,
	abort: Option<&'a CancellationToken>,
}

#[cfg(diskann)]
impl crate::catalog::providers::CancellationProbe for PassEnv<'_> {
	fn expect_not_timedout(&self) -> crate::catalog::providers::BoxProviderFut<'_, Result<()>> {
		self.inner.expect_not_timedout()
	}
}

#[cfg(diskann)]
impl crate::idx::env::IndexEnv for PassEnv<'_> {
	fn tx(&self) -> Arc<Transaction> {
		self.inner.tx()
	}

	fn is_done(&self, count: Option<usize>) -> crate::idx::env::BoxEnvFut<'_> {
		match self.abort {
			Some(abort) if abort.is_cancelled() => Box::pin(async { Ok(true) }),
			Some(_) => self.inner.is_done(count),
			// Nothing is to stop the frame: background compaction stops at plan
			// boundaries, and requeues what it leaves.
			None => Box::pin(async { Ok(false) }),
		}
	}

	fn index_stores(&self) -> &crate::idx::trees::store::IndexStores {
		self.inner.index_stores()
	}

	fn node_id(&self) -> Uuid {
		self.inner.node_id()
	}

	fn sequences(&self) -> Result<&Sequences> {
		self.inner.sequences()
	}

	fn config(&self) -> &crate::idx::config::IdxConfig {
		self.inner.config()
	}

	fn places(&self) -> crate::idx::places::Ticket {
		self.inner.places()
	}
}

/// Runs the compaction passes over one index at a time.
///
/// Built from a context, which supplies what a pass needs: the transaction
/// factory and sequences, the index stores and their configuration, and the
/// node's id. A pass reads and writes in transactions of its own, never in the
/// context's, and answers to no deadline or cancellation the context carries:
/// it stops on the canceller its caller hands it, at a plan boundary, or — when
/// built [`Materialiser::with_frame_abort`] and that token is tripped — inside
/// the DiskANN plan it is applying, which then commits nothing. Either way every
/// transaction it opened is ended, none abandoned part-way.
#[derive(Clone)]
pub(crate) struct Materialiser {
	/// The context every pass's transactions are attached to, detached from
	/// the deadline, the cancellation and the transaction of the context the
	/// materialiser was built from.
	ctx: FrozenContext,
	/// The node's index builder, for its transaction factory and its
	/// per-index materialise locks.
	builder: IndexBuilder,
	sequences: Sequences,
	node_id: Uuid,
	/// Tripped when the work running this materialiser is cancelled outright,
	/// rather than ended at a deadline or by shutdown: a frame in progress then
	/// stops at its next check and commits nothing, where the pass's own
	/// canceller only stops it at its next plan boundary. `None` lets every
	/// frame run to its end.
	abort_frames: Option<CancellationToken>,
	/// What scheduled the passes this materialiser runs.
	trigger: IndexCompactionTrigger,
}

impl Materialiser {
	/// A materialiser running its passes from `ctx`'s datastore.
	///
	/// # Errors
	/// When `ctx` carries no index builder or no sequences, as a context of a
	/// datastore always does.
	pub(crate) fn new(ctx: &FrozenContext) -> Result<Self> {
		let builder = ctx
			.get_index_builder()
			.ok_or_else(|| EngineError::unreachable("No Index Builder"))?
			.clone();
		let sequences = ctx.try_get_sequences()?.clone();
		Ok(Self {
			ctx: Context::new_concurrent(ctx).freeze(),
			builder,
			sequences,
			node_id: ctx.node_id(),
			abort_frames: None,
			trigger: IndexCompactionTrigger::Queue,
		})
	}

	/// The same materialiser, whose frames in progress stop once `abort` is
	/// tripped.
	pub(crate) fn with_frame_abort(mut self, abort: CancellationToken) -> Self {
		self.abort_frames = Some(abort);
		self
	}

	/// Sets the compaction trigger reported on pass completion events.
	pub(crate) fn with_trigger(mut self, trigger: IndexCompactionTrigger) -> Self {
		self.trigger = trigger;
		self
	}

	async fn transaction(&self, tt: TransactionType) -> Result<Transaction> {
		self.builder.transaction_factory().transaction(tt, self.sequences.clone()).await
	}

	/// Release every writer lease this node holds on index `ikb`, in every guard family, and
	/// answer how many it released. A lease is only held on a store this datastore is not the
	/// sole writer of, so elsewhere this releases nothing.
	async fn release_writer_leases(&self, ikb: &IndexKeyBase) -> Result<usize> {
		let index = ikb.guarded(surrealdb_datastore::pending_guard::PendingGuardFamily::Hnsw).index;
		self.builder.transaction_factory().release_writer_leases_of(&self.sequences, &index).await
	}

	/// The process-local mutex for `ikb`'s passes. A caller holds it across a
	/// pass, so two passes in this process never plan the same work.
	pub(crate) fn lock(&self, ikb: &IndexKeyBase) -> Arc<tokio::sync::Mutex<()>> {
		self.builder.materialise_lock(ikb)
	}

	/// Runs one pass over `ikb` under its materialise lock, waiting for the
	/// lock first, and reports how it ended (see [`Self::run_pass_held`]).
	pub(crate) async fn run_pass(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		lease: Option<&dyn PassLease>,
		places: &crate::idx::places::Ticket,
	) -> CompactionPassReport {
		let lock = self.lock(ikb);
		// A cancelled caller stops waiting for a pass this process is already
		// running, rather than queueing behind it.
		let _held = tokio::select! {
			biased;
			_ = canceller.cancelled() => {
				return CompactionPassReport {
					result: Err(EngineError::QueryCancelled.into()),
					unqueued: false,
				};
			}
			held = lock.lock() => held,
		};
		self.run_pass_held(ikb, canceller, lease, places).await
	}

	/// Withdraw the lapsed `Online` generation of the index `ikb` names, built
	/// against what the catalog does not describe, in a transaction of its own
	/// ([`clear_lapsed_online_owner`]).
	///
	/// # Returns
	/// Whether it withdrew. A retryable conflict answers `false`, and so does an
	/// owner that is no longer lapsed or a basis the catalog now describes: the
	/// request is kept for the next pass.
	async fn withdraw_stale_build(&self, ikb: &IndexKeyBase) -> Result<bool> {
		let tx = self.transaction(Write).await?;
		let staged = async {
			let BuildOwner::Lapsed(state) = build_owner(&tx, ikb, true).await? else {
				return Ok(false);
			};
			// Answers whether it withdrew. A clear that kept the generation
			// `Online` is not this pass's to commit: it is cancelled below, and
			// the apply's write guard clears that owner.
			clear_lapsed_online_owner(&tx, ikb, &state).await
		}
		.await;
		let withdrew = match staged {
			Ok(true) => tx.commit().await.map(|()| true),
			Ok(false) => {
				let _ = tx.cancel().await;
				return Ok(false);
			}
			Err(e) => {
				let _ = tx.cancel().await;
				Err(e)
			}
		};
		match withdrew {
			Err(e) if is_retryable_transaction_conflict(&e) => Ok(false),
			withdrew => withdrew,
		}
	}

	/// Retire the lapsed build of the index `ikb` names, whose definition never
	/// committed, in a transaction of its own ([`retire_uncommitted_build`]).
	///
	/// # Returns
	/// Whether it retired. A retryable conflict answers `false`, and so does a
	/// definition or a heartbeat that landed first: the request is kept for the
	/// next pass.
	async fn retire_uncommitted_build(&self, ikb: &IndexKeyBase) -> Result<bool> {
		let tx = self.transaction(Write).await?;
		let retired = match retire_uncommitted_build(&tx, ikb).await {
			Ok(true) => tx.commit().await.map(|()| true),
			Ok(false) => {
				let _ = tx.cancel().await;
				return Ok(false);
			}
			Err(e) => {
				let _ = tx.cancel().await;
				Err(e)
			}
		};
		match retired {
			Err(e) if is_retryable_transaction_conflict(&e) => Ok(false),
			retired => retired,
		}
	}

	/// Runs one compaction pass over a single index, and reports how it ended.
	/// The caller holds the index's materialise lock ([`Self::lock`]).
	///
	/// Looks up the index definition identified by `ikb` and dispatches to
	/// the appropriate compaction implementation based on the index type:
	/// full-text, count, HNSW, or DiskANN. An index that is being removed
	/// (`prepare_remove`), not found, or of an unsupported type ends the pass
	/// on [`CompactionPassOutcome::Empty`].
	///
	/// A pass applies at most [`INDEX_COMPACTION_PASS_PLAN_BUDGET`] plans and
	/// makes at most [`INDEX_COMPACTION_PASS_MAX_RETRIES`] retries, each either a
	/// retryable conflict or a plan that lost its race and is read again, and a
	/// pass given a `lease` stops at its next plan boundary once the lease is
	/// found lost. One that stops with work left
	/// ([`CompactionPassOutcome::Budget`]), loses a race it has no retry left
	/// for ([`CompactionPassOutcome::LostRace`]), or fails on a retryable
	/// conflict it has no retry left for, queues a fresh compaction request for
	/// its index before it returns — the request a write to the index queues —
	/// so the index is compacted again without waiting for its next write, and
	/// the report says when that request failed to commit. A pass that fails on
	/// anything else queues nothing. The kinds that run work in parallel take
	/// its places through `places`.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "debug",
			target = "surrealdb::core::kvs::index::materialise",
			skip_all,
			fields(
				ns = %ikb.ns(),
				db = %ikb.db(),
				tb = %ikb.table(),
				ix = ?ikb.index(),
				trigger = self.trigger.as_label()
			)
		)
	)]
	pub(crate) async fn run_pass_held(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		lease: Option<&dyn PassLease>,
		places: &crate::idx::places::Ticket,
	) -> CompactionPassReport {
		let _active = self.builder.enter_pass(ikb);
		let started = Instant::now();
		let mut kind = None;
		let result = self.index_compaction_pass(ikb, canceller, lease, places, &mut kind).await;
		// Read before the requeue below awaits, so a cancellation that trips
		// during that cleanup does not label an earlier failure `cancelled`.
		let cancelled = canceller.is_cancelled();
		let owes_request = match &result {
			Ok(outcome) => {
				matches!(outcome, CompactionPassOutcome::Budget | CompactionPassOutcome::LostRace)
			}
			Err(e) => is_retryable_transaction_conflict(e),
		};
		let unqueued = owes_request
			&& match self.requeue_index_compaction(ikb).await {
				Ok(()) => false,
				Err(e) => {
					warn!(target: TARGET, "Failed to requeue index compaction for {ikb}: {e}");
					true
				}
			};
		let outcome = match &result {
			Ok(outcome) => outcome.observe_outcome(),
			Err(_) if cancelled => IndexCompactionPassOutcome::Cancelled,
			Err(_) => IndexCompactionPassOutcome::Error,
		};
		self.builder.transaction_factory().observer().on_index_compaction_pass(
			&IndexCompactionPassEvent {
				safe: IndexCompactionPassEventSafe {
					kind,
					trigger: self.trigger,
					outcome,
					duration: started.elapsed(),
					unqueued,
				},
				ctx: IndexCompactionPassEventCtx::default(),
			},
		);
		CompactionPassReport {
			result,
			unqueued,
		}
	}

	/// Queues a fresh compaction request for one index, under the key a write
	/// to the index queues ([`IndexOperation::compaction_trigger`]), and wakes
	/// the compactor when it commits.
	///
	/// A retryable conflict is retried, at most
	/// [`INDEX_COMPACTION_PASS_MAX_RETRIES`] times and after the same wait as a
	/// pass's retries, so only a failure that outlasts them leaves the index
	/// without its request.
	pub(crate) async fn requeue_index_compaction(&self, ikb: &IndexKeyBase) -> Result<()> {
		let mut retries = 0;
		loop {
			let res = self.try_requeue_index_compaction(ikb).await;
			match res {
				Err(e)
					if retries < INDEX_COMPACTION_PASS_MAX_RETRIES
						&& Datastore::retry_index_operation_conflict(
							&e,
							format!(
								"Retryable conflict requeuing index compaction for {ikb}, retrying"
							),
						)
						.await =>
				{
					retries += 1;
				}
				res => return res,
			}
		}
	}

	/// One attempt of [`Self::requeue_index_compaction`].
	///
	/// Each cancel here ignores its own error: the failure before it is what
	/// the attempt returns, and a request that has not committed is not queued,
	/// so a cancel that fails changes nothing the caller reads.
	async fn try_requeue_index_compaction(&self, ikb: &IndexKeyBase) -> Result<()> {
		let txn = self.transaction(Write).await?;
		if let Err(e) = IndexOperation::compaction_trigger(ikb, &txn, self.node_id).await {
			let _ = txn.cancel().await;
			return Err(e);
		}
		#[cfg(test)]
		if let Err(e) = maybe_inject_non_retryable_error(
			NonRetryableErrorSite::IndexCompactionRequeue,
			self.node_id,
		) {
			let _ = txn.cancel().await;
			return Err(e);
		}
		#[cfg(test)]
		if let Err(e) = maybe_inject_retryable_conflict(
			RetryableConflictSite::IndexCompactionRequeue,
			self.node_id,
		) {
			let _ = txn.cancel().await;
			return Err(e);
		}
		txn.commit().await
	}

	/// Dispatches one pass over a single index to its kind's pass.
	async fn index_compaction_pass(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		lease: Option<&dyn PassLease>,
		places: &crate::idx::places::Ticket,
		kind_out: &mut Option<IndexCompactionKind>,
	) -> Result<CompactionPassOutcome> {
		Datastore::ensure_not_cancelled(canceller)?;
		// The catalog definition and the index's durable build state, read
		// together so the ownership decision below costs no extra transaction.
		let (ix, owner, lapsed, stale) = {
			let txn = self.transaction(Read).await?;
			let res = async {
				let ix = txn
					.get_tb_index_by_id(ikb.ns(), ikb.db(), ikb.table(), ikb.index(), None)
					.await?;
				let state = txn.get_key(&ikb.new_bs_key(), None).await?;
				// Retirable only where the store lets a lapsed owner be cleared,
				// so a store that does not opens no write transaction for it.
				let lapsed = match (&ix, &state) {
					(None, Some(state)) if uncommitted_build_lapsed(state, chrono::Utc::now()) => {
						lapsed_online_owners_clear(&txn).await?
					}
					_ => false,
				};
				let live = build_owner_of(&txn, state).await?;
				// A lapsed owner of a generation built against what the
				// catalog does not describe, which this pass withdraws.
				let stale = match &live {
					BuildOwner::Lapsed(state) if ix.is_some() => {
						build_basis_is_stale(&txn, ikb, state).await?
					}
					BuildOwner::Released | BuildOwner::Held | BuildOwner::Lapsed(_) => false,
				};
				Ok::<_, anyhow::Error>((ix, live, lapsed, stale))
			}
			.await;
			let _ = txn.cancel().await;
			res?
		};
		Datastore::ensure_not_cancelled(canceller)?;
		// A request is deleted only by a pass that could act on it. A pass that
		// cannot reports an outcome that
		// [`CompactionPassOutcome::retains_request`], and the batch keeps one of
		// the index's requests in place, deleting the rest; keeping it writes
		// nothing and wakes nothing, so it waits for the next pass that reads
		// the queue. Whatever ends the condition — a release, a takeover, the
		// commit of a defining statement, the clearing of a stranded owner —
		// then finds the request already there, so no request is lost. A
		// release also queues a request of its own, in the transaction that
		// commits it, so that its commit wakes the compactor
		// (`Building::requeue_compaction_with_release`); every other condition
		// leaves the kept request to the next drain.
		//
		// An index with no readable definition is one of two things. With a
		// live build owner, it is being defined by a statement that has not
		// committed: the build writes deltas and requests under the index's id
		// meanwhile, and the definition becomes readable when the statement
		// commits, so the request is kept. Without one, the index was removed or
		// its defining statement rolled back, and its requests are dropped. A
		// removed index whose build state still names a live owner keeps its
		// request until that state is retired, and a table recreated under an
		// id the retained request names costs that index one pass with nothing
		// to fold.
		//
		// An owner whose lease lapsed may belong to a statement that will never
		// commit, its node gone. The resume scan visits only the indexes the
		// catalog defines, so this pass retires that build, fencing the
		// statement in the same commit ([`retire_uncommitted_build`]), and drops
		// the request.
		let ix = match ix {
			Some(ix) if !ix.prepare_remove => ix,
			Some(_) => {
				trace!(target: TARGET, "Index compaction: Index {ikb:?} is being removed, skipping");
				self.builder.forget_pending_deletes(ikb);
				return Ok(CompactionPassOutcome::Empty);
			}
			None if !matches!(owner, BuildOwner::Released) => {
				if lapsed && self.retire_uncommitted_build(ikb).await? {
					trace!(target: TARGET, "Index compaction: Index {ikb:?} was never defined and its build lapsed, retired it");
					self.builder.forget_pending_deletes(ikb);
					return Ok(CompactionPassOutcome::Empty);
				}
				trace!(target: TARGET, "Index compaction: Index {ikb:?} is not yet defined, keeping its request");
				return Ok(CompactionPassOutcome::NotVisible);
			}
			None => {
				trace!(target: TARGET, "Index compaction: Index {ikb:?} not found, skipping");
				self.builder.forget_pending_deletes(ikb);
				return Ok(CompactionPassOutcome::Empty);
			}
		};
		*kind_out = compaction_kind(&ix.index);
		// A lapsed generation built against what the catalog does not describe
		// is withdrawn rather than folded through the definition the catalog
		// holds, and its request dropped: no query reads a withdrawn
		// generation, and the next build of the index starts over. One the
		// withdrawal could not settle keeps its request for the next pass.
		if stale {
			return if self.withdraw_stale_build(ikb).await? {
				trace!(target: TARGET, "Index compaction: Index {ikb:?} had a lapsed build its catalog does not describe, withdrew it");
				Ok(CompactionPassOutcome::Empty)
			} else {
				Ok(CompactionPassOutcome::Fenced)
			};
		}
		let Some(kind) = *kind_out else {
			self.builder.forget_pending_deletes(ikb);
			trace!(target: TARGET, "Index compaction: Index {:?} does not support compaction, skipping", ikb);
			return Ok(CompactionPassOutcome::Empty);
		};
		let mut progress = CompactionPassProgress::new(
			kind,
			Arc::clone(self.builder.transaction_factory().observer()),
		);
		// A vector index whose build is still in flight is left alone until
		// its owner releases it, because the catalog this pass reads may name
		// a layout the build is replacing. A `REBUILD INDEX` that migrates
		// `format_version` holds the new definition in its uncommitted
		// statement transaction, so the catalog read above still reports the
		// retired one. Compacting from here would fold the pendings into the
		// retired layout and leave the published index without them; and
		// because the process-local vector-index wrapper is keyed without the
		// format version, two compactors reading two layouts would also evict
		// each other's graph on every access.
		//
		// Durable state is the whole test, on every node including the one
		// running the build. It is committed before the builder is
		// registered locally and held until the statement that publishes
		// the definition closes its transaction, so it already covers every
		// window in which this catalog read can be stale.
		//
		// The fence is scoped to the kinds whose fold depends on the
		// definition's layout ([`fenced_by_build_owner`]). Full-text and count
		// passes run under a live owner and fold what a build has written.
		// The fenced pass keeps one request for the index, and the release
		// queues one, so everything the build and writers queued is folded by
		// the first pass after the fence comes down.
		// An `Online` owner whose heartbeat lapsed is let through to the apply,
		// whose write guard clears it and fences its statement in the same
		// transaction, or finds it held after all.
		if fenced_by_build_owner(&ix.index) && matches!(owner, BuildOwner::Held) {
			trace!(target: TARGET, "Index compaction: Index {ikb:?} is being built, leaving it to its builder");
			return Ok(CompactionPassOutcome::Fenced);
		}
		if !matches!(&ix.index, Index::Hnsw(_)) {
			self.builder.forget_pending_deletes(ikb);
		}
		match &ix.index {
			Index::FullText(_) => {
				self.process_fulltext_compaction(ikb, canceller, lease, &mut progress).await
			}
			Index::Count(_) => {
				self.process_count_compaction(ikb, canceller, lease, &mut progress).await
			}
			// HNSW compaction owns its pending-key allocation and pending-range
			// semantics separately from full-text/count compaction.
			Index::Hnsw(_) => {
				self.process_hnsw_compaction(ikb, canceller, lease, places, &mut progress).await
			}
			#[cfg(diskann)]
			Index::DiskAnn(_) => {
				self.process_diskann_compaction(ikb, canceller, lease, places, &mut progress).await
			}
			_ => Ok(CompactionPassOutcome::Empty),
		}
	}

	/// Discards the process-local HNSW wrapper and element caches for one index
	/// after a compaction write that reached the graph and did not commit.
	///
	/// Neither half of that state can be validated against the store. The
	/// element caches carry no version at all. A graph layer carries one, but
	/// it is a counter each writer advances in its own copy as it applies, so
	/// two writers applying the same batch from the same version reach the same
	/// one: the rolled-back topology is then indistinguishable to a state check
	/// from the topology the winning commit stored, and a later apply would
	/// write node changes derived from it over the winner's nodes. An apply
	/// that failed part-way is worse still — it can mutate a layer before
	/// reaching the `save_nodes` that advances the version at all.
	///
	/// The next access rebuilds the wrapper and reloads every layer from
	/// persisted state, which is the cost this pays. A write that stopped at
	/// the compaction guards never reached the graph and must not come here.
	async fn discard_hnsw_index(
		&self,
		tb: crate::catalog::TableId,
		ikb: &IndexKeyBase,
		format_version: u16,
	) {
		if let Err(e) =
			self.ctx.get_index_stores().remove_hnsw_index(tb, ikb.clone(), format_version).await
		{
			warn!(target: TARGET, "Failed to evict HNSW index after an uncommitted compaction write: {e}");
		}
	}

	/// Runs once a pass has found an HNSW index's pending ranges empty and the
	/// pass's cancellation and lease still hold.
	async fn after_hnsw_drain(
		&self,
		ikb: &IndexKeyBase,
		format_version: u16,
		canceller: &CancellationToken,
		lease: Option<&dyn PassLease>,
	) {
		if !canceller.is_cancelled()
			&& !lease.is_some_and(|lease| lease.is_lost())
			&& self.builder.take_pending_range_compaction(ikb)
		{
			self.compact_pending_range(ikb, format_version).await;
		}
	}

	/// Runs a storage-layer compaction over one HNSW index's pending ranges
	/// (`!hr`, `!hw`, and legacy `!hp`).
	///
	/// Every batch of [`Self::process_hnsw_compaction`] scans the pending ranges
	/// in a read transaction and then deletes the keys it captured. After a
	/// large backlog has been drained, the keyspace is logically empty while
	/// the storage engine may still hold the deleted keys' tombstones across
	/// its immutable memtables and SSTs; every subsequent scan walks past all
	/// of them to learn there is nothing left. Asking the engine to compact
	/// these key ranges folds the tombstones into the SSTs they shadow so later
	/// scans return without traversing them.
	///
	/// Best-effort: engines without range compaction (in-memory, browser,
	/// FoundationDB) treat `compact` as a no-op, and any error is logged and
	/// ignored because correctness does not depend on tombstone cleanup.
	async fn compact_pending_range(&self, ikb: &IndexKeyBase, format_version: u16) {
		let hr_range = match ikb.new_hr_range() {
			Ok(range) => range,
			Err(e) => {
				warn!(target: TARGET, "Failed to build HNSW record-pending range for {ikb}: {e}");
				return;
			}
		};
		let hw_range = if crate::catalog::guards_hnsw_pending(format_version) {
			match ikb.new_hw_range() {
				Ok(range) => Some(range),
				Err(e) => {
					warn!(
						target: TARGET,
						"Failed to build HNSW sharded record-pending range for {ikb}: {e}"
					);
					return;
				}
			}
		} else {
			None
		};
		let hp_range = if crate::catalog::may_hold_legacy_vector_pending(format_version) {
			match ikb.new_hp_range() {
				Ok(range) => Some(range),
				Err(e) => {
					warn!(
						target: TARGET,
						"Failed to build HNSW element-pending range for {ikb}: {e}"
					);
					return;
				}
			}
		} else {
			None
		};
		let txn = match self.transaction(Read).await {
			Ok(txn) => txn,
			Err(e) => {
				warn!(
					target: TARGET,
					"Failed to open transaction to compact HNSW pending range for {ikb}: {e}"
				);
				return;
			}
		};
		if let Err(e) = txn.compact_raw(hr_range).await {
			warn!(target: TARGET, "Failed to compact HNSW record-pending range for {ikb}: {e}");
		}
		if let Some(hw_range) = hw_range
			&& let Err(e) = txn.compact_raw(hw_range).await
		{
			warn!(
				target: TARGET,
				"Failed to compact HNSW sharded record-pending range for {ikb}: {e}"
			);
		}
		if let Some(hp_range) = hp_range
			&& let Err(e) = txn.compact_raw(hp_range).await
		{
			warn!(target: TARGET, "Failed to compact HNSW element-pending range for {ikb}: {e}");
		}
		let _ = txn.cancel().await;
	}

	/// Runs HNSW compaction as bounded read-plan/write-apply batches.
	///
	/// Pending entries are captured in a read transaction and conditionally
	/// deleted in a short write transaction before graph mutation. A write that
	/// reached the graph in place and then did not commit leaves the
	/// process-local wrapper holding a topology and caches the store never
	/// took, so the wrapper is discarded; a write that was staged, or that
	/// stopped at the guards, left the wrapper untouched and keeps it. The
	/// discard targets the format version the write phase applied through — a
	/// `REBUILD INDEX` can bump the catalog format after the pass was
	/// dispatched, and both the wrapper and the cache entries are scoped per
	/// format.
	///
	/// A cancel this pass makes after a step fails, or after a plan turns out
	/// to have nothing to apply, ignores its own error: the step's error or the
	/// pass's outcome is what the pass acts on and reports, and the transaction
	/// it cancels has not committed, so a cancel that fails discards nothing
	/// that could apply.
	async fn process_hnsw_compaction(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		lease: Option<&dyn PassLease>,
		places: &crate::idx::places::Ticket,
		progress: &mut CompactionPassProgress,
	) -> Result<CompactionPassOutcome> {
		loop {
			let plan_started = Instant::now();
			Datastore::ensure_not_cancelled(canceller)?;
			if lease.is_some_and(|lease| lease.is_lost()) {
				return Ok(CompactionPassOutcome::Budget);
			}
			let prepared = {
				let txn = Arc::new(self.transaction(Read).await?);
				let res: Result<
					Option<(
						crate::catalog::TableId,
						crate::idx::trees::hnsw::index::HnswCompactionPlan,
						u16,
					)>,
				> = async {
					let Some(tb) = txn.get_tb(ikb.ns(), ikb.db(), ikb.table(), None).await? else {
						return Ok(None);
					};
					match txn
						.get_tb_index_by_id(ikb.ns(), ikb.db(), ikb.table(), ikb.index(), None)
						.await?
					{
						Some(ix) if !ix.prepare_remove && matches!(&ix.index, Index::Hnsw(_)) => {
							let mut ctx = Context::new_child(&self.ctx);
							ctx.set_transaction(Arc::clone(&txn));
							let ctx = ctx.freeze();
							let plan = IndexOperation::prepare_hnsw_compaction(
								&ctx,
								ikb,
								ix.format_version,
							)
							.await?;
							Ok(Some((tb.table_id, plan, ix.format_version)))
						}
						_ => Ok(None),
					}
				}
				.await;
				let _ = txn.cancel().await;
				res?
			};
			let Some((tb, plan, format_version)) = prepared else {
				self.builder.forget_pending_deletes(ikb);
				return Ok(progress.nothing_to_apply());
			};
			if !plan.requires_apply() {
				self.after_hnsw_drain(ikb, format_version, canceller, lease).await;
				return Ok(progress.nothing_to_apply());
			}
			let has_more = plan.has_more();
			let captured = plan.captured_len();
			// Run once the plan's transaction has finished: the apply does not
			// commit it, and a staged apply publishes from that commit, so the
			// guards of the shards it drained are stepped only after it.
			let guard_step = plan.guard_step(ikb);
			Datastore::ensure_not_cancelled(canceller)?;

			let txn = Arc::new(self.transaction(Write).await?);
			let ix = match compaction_write_guard(&txn, ikb).await {
				Ok(CompactionWriteGuard::Live(ix)) => ix,
				Ok(CompactionWriteGuard::Retired) => {
					let _ = txn.cancel().await;
					self.builder.forget_pending_deletes(ikb);
					return Ok(progress.nothing_to_apply());
				}
				Ok(CompactionWriteGuard::Fenced) => {
					let _ = txn.cancel().await;
					return Ok(CompactionPassOutcome::Fenced);
				}
				Err(e) => {
					let _ = txn.cancel().await;
					if progress
						.retry(
							&e,
							format!(
								"Retryable conflict guarding HNSW compaction for {ikb}, retrying"
							),
						)
						.await
					{
						continue;
					}
					return Err(e);
				}
			};
			let Index::Hnsw(p) = &ix.index else {
				let _ = txn.cancel().await;
				self.builder.forget_pending_deletes(ikb);
				return Ok(progress.nothing_to_apply());
			};
			// The apply mutates the wrapper and cache of the format the guard
			// read, so every failure-side recovery below clears that format.
			let applied_format = ix.format_version;
			let res = {
				let mut ctx = Context::new_child(&self.ctx);
				ctx.set_transaction(Arc::clone(&txn));
				ctx.set_index_places(places.clone());
				let ctx = ctx.freeze();
				IndexOperation::apply_hnsw_compaction(
					&ctx,
					self.ctx.get_index_stores(),
					ikb,
					p,
					applied_format,
					plan,
				)
				.await
			};
			match res {
				Ok(outcome) if outcome.applied() => {
					// Only an in-place batch reaches the graph before its commit; a
					// staged one publishes from the commit itself, so a transaction
					// that does not commit leaves its wrapper untouched.
					let in_place = outcome == HnswCompactionOutcome::InPlace;
					let plan_mode = if in_place {
						IndexCompactionPlanMode::InPlace
					} else {
						IndexCompactionPlanMode::Staged
					};
					if let Err(e) = Datastore::ensure_not_cancelled(canceller) {
						let _ = txn.cancel().await;
						if in_place {
							self.discard_hnsw_index(tb, ikb, applied_format).await;
							progress.record_discarded_cache();
						}
						return Err(e);
					}
					#[cfg(test)]
					if let Err(e) = maybe_inject_non_retryable_error(
						NonRetryableErrorSite::HnswCompaction,
						self.node_id,
					) {
						let _ = txn.cancel().await;
						if in_place {
							self.discard_hnsw_index(tb, ikb, applied_format).await;
							progress.record_discarded_cache();
						}
						return Err(e);
					}
					#[cfg(test)]
					if let Err(e) = maybe_inject_retryable_conflict(
						RetryableConflictSite::HnswCompaction,
						self.node_id,
					) {
						let _ = txn.cancel().await;
						if in_place {
							self.discard_hnsw_index(tb, ikb, applied_format).await;
							progress.record_discarded_cache();
						}
						if progress
							.retry(
								&e,
								format!(
									"Retryable conflict committing HNSW compaction for {ikb}, retrying"
								),
							)
							.await
						{
							continue;
						}
						return Err(e);
					}
					if let Err(e) = txn.commit().await {
						let _ = txn.cancel().await;
						if in_place {
							self.discard_hnsw_index(tb, ikb, applied_format).await;
							progress.record_discarded_cache();
						}
						if progress
							.retry(
								&e,
								format!(
									"Retryable conflict committing HNSW compaction for {ikb}, retrying"
								),
							)
							.await
						{
							continue;
						}
						return Err(e);
					}
					self.builder.note_pending_deleted(ikb, captured);
					// The apply has committed, so a failed step does not undo it: the
					// guards are left as they were, which may only make lookups scan a
					// drained shard, and the next pass steps them.
					if let Err(e) = guard_step.run(&txn).await {
						warn!(
							target: TARGET,
							"Failed to step the HNSW pending-state guards after a compaction of {ikb}: {e}"
						);
					}
					let outcome = progress.record_applied(
						plan_mode,
						captured,
						plan_started.elapsed(),
						has_more,
					);
					Datastore::ensure_not_cancelled(canceller)?;
					if let Some(outcome) = outcome {
						if !has_more {
							self.after_hnsw_drain(ikb, applied_format, canceller, lease).await;
						}
						return Ok(outcome);
					}
				}
				Ok(HnswCompactionOutcome::LostRace) => {
					let _ = txn.cancel().await;
					if progress.retry_lost_race(ikb) {
						continue;
					}
					return Ok(CompactionPassOutcome::LostRace);
				}
				Ok(_) => {
					let _ = txn.cancel().await;
					// The plan captured nothing, but may have drained shards, an
					// armed summary over guards that all read `Empty`, or repaired
					// an orphaned shard guard for the next frame, to step.
					match guard_step.run(&txn).await {
						Ok(true) => {}
						Ok(false) if has_more => {}
						Ok(false) => {
							self.after_hnsw_drain(ikb, applied_format, canceller, lease).await;
							return Ok(progress.nothing_to_apply());
						}
						Err(e) => {
							if progress
								.retry(
									&e,
									format!(
										"Retryable conflict stepping HNSW pending-state guards for {ikb}, retrying"
									),
								)
								.await
							{
								continue;
							}
							return Err(e);
						}
					}
					Datastore::ensure_not_cancelled(canceller)?;
					// A plan that captured nothing stopped at its cap counts toward
					// the pass's plan budget, as a DiskANN frame that applied nothing
					// with entries behind it does. A guard step with nothing behind
					// it is not a plan: the pass replans, and ends once a plan finds
					// nothing left to step.
					if has_more
						&& let Some(outcome) = progress.record_applied(
							IndexCompactionPlanMode::Folded,
							0,
							plan_started.elapsed(),
							has_more,
						) {
						return Ok(outcome);
					}
				}
				Err(err) => {
					let _ = txn.cancel().await;
					if err.in_place {
						self.discard_hnsw_index(tb, ikb, applied_format).await;
						progress.record_discarded_cache();
					}
					if progress
						.retry(
							&err.error,
							format!(
								"Retryable conflict applying HNSW compaction for {ikb}, retrying"
							),
						)
						.await
					{
						continue;
					}
					return Err(err.error);
				}
			}
		}
	}

	#[cfg(diskann)]
	/// Runs DiskANN compaction as bounded read-plan/write-apply batches.
	///
	/// Each plan is one frame, run in a task of its own that this pass awaits
	/// ([`Self::diskann_frame`]): prepare the plan, open the write
	/// transaction, guard it, apply and commit or cancel. A frame holds a write
	/// transaction with buffered writes, the graph lock and places, and none of
	/// them may be left without an owner because a caller stopped polling, so a
	/// caller that drops this pass leaves at most that one frame to finish. The
	/// frame stops early, and commits nothing, once the materialiser's frame
	/// abort is tripped ([`Self::with_frame_abort`]); `canceller` stops the pass
	/// between frames.
	///
	/// `apply_diskann_compaction` commits its own transaction, so a commit
	/// conflict arrives as its error, which is retried like the other kinds'
	/// commit conflicts.
	async fn process_diskann_compaction(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		lease: Option<&dyn PassLease>,
		places: &crate::idx::places::Ticket,
		progress: &mut CompactionPassProgress,
	) -> Result<CompactionPassOutcome> {
		loop {
			Datastore::ensure_not_cancelled(canceller)?;
			if lease.is_some_and(|lease| lease.is_lost()) {
				return Ok(CompactionPassOutcome::Budget);
			}
			#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
			let step = {
				let frame = {
					let this = self.clone();
					let ikb = ikb.clone();
					let canceller = canceller.clone();
					let places = places.clone();
					common::rt::spawn(
						async move { this.diskann_frame(&ikb, &canceller, &places).await },
					)
				};
				match frame.await {
					Ok(step) => step,
					Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
					Err(e) => Err(DiskAnnFrameError::Plan(anyhow::anyhow!(
						"DiskANN compaction frame for {ikb} did not complete: {e}"
					))),
				}
			};
			// The browser executor gives no join handle, so the detached frame sends its
			// step back; it still runs to the end if this pass is dropped.
			#[cfg(all(target_family = "wasm", target_os = "unknown"))]
			let step = {
				let (done, wait) = tokio::sync::oneshot::channel();
				let this = self.clone();
				let frame_ikb = ikb.clone();
				let canceller = canceller.clone();
				let places = places.clone();
				common::rt::spawn_detached(async move {
					// The send fails only when this pass was dropped while the frame ran. The
					// frame has finished its own transaction by then, and nobody is left to
					// act on its step, so there is nothing to do with the error.
					let _ = done.send(this.diskann_frame(&frame_ikb, &canceller, &places).await);
				});
				wait.await.unwrap_or_else(|_| {
					Err(DiskAnnFrameError::Plan(anyhow::anyhow!(
						"DiskANN compaction frame for {ikb} did not complete"
					)))
				})
			};
			let (has_more, keys_compacted, duration) = match step {
				Ok(DiskAnnFrameStep::Applied {
					has_more,
					keys_compacted,
					duration,
				}) => (has_more, keys_compacted, duration),
				Ok(DiskAnnFrameStep::Empty) => return Ok(progress.nothing_to_apply()),
				Ok(DiskAnnFrameStep::Fenced) => return Ok(CompactionPassOutcome::Fenced),
				Ok(DiskAnnFrameStep::LostRace) => {
					if progress.retry_lost_race(ikb) {
						continue;
					}
					return Ok(CompactionPassOutcome::LostRace);
				}
				Err(DiskAnnFrameError::Plan(e)) => return Err(e),
				Err(DiskAnnFrameError::Guard(e)) => {
					if progress
						.retry(
							&e,
							format!(
								"Retryable conflict guarding DiskANN compaction for {ikb}, retrying"
							),
						)
						.await
					{
						continue;
					}
					return Err(e);
				}
				Err(DiskAnnFrameError::Apply(e)) => {
					if progress
						.retry(
							&e,
							format!(
								"Retryable conflict applying DiskANN compaction for {ikb}, retrying"
							),
						)
						.await
					{
						continue;
					}
					return Err(e);
				}
			};
			let outcome = progress.record_applied(
				IndexCompactionPlanMode::Folded,
				keys_compacted,
				duration,
				has_more,
			);
			Datastore::ensure_not_cancelled(canceller)?;
			if let Some(outcome) = outcome {
				return Ok(outcome);
			}
		}
	}

	#[cfg(diskann)]
	/// One frame of [`Self::process_diskann_compaction`]: prepares a plan on a
	/// read transaction, then guards and applies it in a write transaction, which
	/// the apply commits or cancels.
	///
	/// A cancel this frame makes after a step fails, or after a plan turns out
	/// to have nothing to apply, ignores its own error: the step's error or the
	/// frame's outcome is what the pass acts on and reports, and the transaction
	/// it cancels has not committed, so a cancel that fails discards nothing
	/// that could apply.
	async fn diskann_frame(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		places: &crate::idx::places::Ticket,
	) -> std::result::Result<DiskAnnFrameStep, DiskAnnFrameError> {
		let frame_started = Instant::now();
		let plan = {
			let txn = Arc::new(self.transaction(Read).await.map_err(DiskAnnFrameError::Plan)?);
			let res: Result<Option<crate::idx::trees::diskann::index::DiskAnnCompactionPlan>> =
				async {
					if txn.get_tb(ikb.ns(), ikb.db(), ikb.table(), None).await?.is_none() {
						return Ok(None);
					}
					match txn
						.get_tb_index_by_id(ikb.ns(), ikb.db(), ikb.table(), ikb.index(), None)
						.await?
					{
						Some(ix)
							if !ix.prepare_remove && matches!(&ix.index, Index::DiskAnn(_)) =>
						{
							let mut ctx = Context::new_child(&self.ctx);
							ctx.set_transaction(Arc::clone(&txn));
							let ctx = ctx.freeze();
							let plan =
								IndexOperation::prepare_diskann_compaction(&ctx, ikb).await?;
							Ok(Some(plan))
						}
						_ => Ok(None),
					}
				}
				.await;
			let _ = txn.cancel().await;
			res.map_err(DiskAnnFrameError::Plan)?
		};
		let Some(plan) = plan else {
			return Ok(DiskAnnFrameStep::Empty);
		};
		if !plan.requires_apply() {
			return Ok(DiskAnnFrameStep::Empty);
		}
		let has_more = plan.has_more();
		let captured = plan.captured_len();
		Datastore::ensure_not_cancelled(canceller).map_err(DiskAnnFrameError::Plan)?;

		let txn = Arc::new(self.transaction(Write).await.map_err(DiskAnnFrameError::Plan)?);
		let ix = match compaction_write_guard(&txn, ikb).await {
			Ok(CompactionWriteGuard::Live(ix)) => ix,
			Ok(CompactionWriteGuard::Retired) => {
				let _ = txn.cancel().await;
				return Ok(DiskAnnFrameStep::Empty);
			}
			Ok(CompactionWriteGuard::Fenced) => {
				let _ = txn.cancel().await;
				return Ok(DiskAnnFrameStep::Fenced);
			}
			Err(e) => {
				let _ = txn.cancel().await;
				return Err(DiskAnnFrameError::Guard(e));
			}
		};
		let Index::DiskAnn(p) = &ix.index else {
			let _ = txn.cancel().await;
			return Ok(DiskAnnFrameStep::Empty);
		};
		// Raised in place of the apply, whose own commit would carry the
		// conflict.
		#[cfg(test)]
		if let Err(e) =
			maybe_inject_non_retryable_error(NonRetryableErrorSite::DiskAnnCompaction, self.node_id)
		{
			let _ = txn.cancel().await;
			return Err(DiskAnnFrameError::Plan(e));
		}
		#[cfg(test)]
		if let Err(e) =
			maybe_inject_retryable_conflict(RetryableConflictSite::DiskAnnCompaction, self.node_id)
		{
			let _ = txn.cancel().await;
			return Err(DiskAnnFrameError::Apply(e));
		}
		let res: Result<CompactionApply> = async {
			let mut ctx = Context::new_child(&self.ctx);
			ctx.set_transaction(Arc::clone(&txn));
			ctx.set_index_places(places.clone());
			let ctx = ctx.freeze();
			let env = PassEnv {
				inner: &*ctx,
				abort: self.abort_frames.as_ref(),
			};
			IndexOperation::apply_diskann_compaction(
				&env,
				self.ctx.get_index_stores(),
				ikb,
				p,
				ix.format_version,
				plan,
			)
			.await
		}
		.await;
		// `apply_diskann_compaction` normally owns the transaction's
		// lifecycle (commits on success, cancels on apply failure while
		// holding the graph write lock). A few pre-apply paths inside
		// `IndexOperation::apply_diskann_compaction` (missing table or
		// catalog lookup errors) can return without finalizing the tx, so
		// this is an idempotent safety net: cancel only if the tx is still
		// open. Cancel on an already-closed tx returns `TransactionFinished`
		// and is harmlessly discarded.
		if !txn.closed() {
			let _ = txn.cancel().await;
		}
		match res {
			Ok(CompactionApply::Applied) => Ok(DiskAnnFrameStep::Applied {
				has_more,
				keys_compacted: captured,
				duration: frame_started.elapsed(),
			}),
			Ok(CompactionApply::Empty) if has_more => Ok(DiskAnnFrameStep::Applied {
				has_more: true,
				keys_compacted: 0,
				duration: frame_started.elapsed(),
			}),
			Ok(CompactionApply::LostRace) => Ok(DiskAnnFrameStep::LostRace),
			Ok(CompactionApply::Empty) => Ok(DiskAnnFrameStep::Empty),
			Err(e) => Err(DiskAnnFrameError::Apply(e)),
		}
	}

	/// Runs full-text compaction as a read-plan followed by a guarded write.
	///
	/// This avoids holding a mutable range scan over `!dc`/`!tt`; deltas
	/// committed after the read snapshot remain for a later compaction.
	///
	/// A cancel this pass makes after a step fails, or after a plan turns out
	/// to have nothing to apply, ignores its own error: the step's error or the
	/// pass's outcome is what the pass acts on and reports, and the transaction
	/// it cancels has not committed, so a cancel that fails discards nothing
	/// that could apply.
	async fn process_fulltext_compaction(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		lease: Option<&dyn PassLease>,
		progress: &mut CompactionPassProgress,
	) -> Result<CompactionPassOutcome> {
		loop {
			let plan_started = Instant::now();
			Datastore::ensure_not_cancelled(canceller)?;
			if lease.is_some_and(|lease| lease.is_lost()) {
				return Ok(CompactionPassOutcome::Budget);
			}
			let plan = {
				let txn = self.transaction(Read).await?;
				// The plan is read under the definition this snapshot holds, as
				// every other kind's is; the apply checks it again under its own
				// locked read.
				let res = async {
					let Some(ix) = txn
						.get_tb_index_by_id(ikb.ns(), ikb.db(), ikb.table(), ikb.index(), None)
						.await?
						.filter(|ix| !ix.prepare_remove)
					else {
						return Ok(None);
					};
					let Index::FullText(p) = &ix.index else {
						return Ok(None);
					};
					IndexOperation::prepare_fulltext_compaction(
						self.ctx.get_index_stores(),
						ikb,
						&txn,
						p,
						&self.ctx.config.idx.file_allowlist,
						ix.format_version,
					)
					.await
					.map(Some)
				}
				.await;
				let _ = txn.cancel().await;
				res?
			};
			let Some(plan) = plan else {
				return Ok(progress.nothing_to_apply());
			};
			if !plan.has_work() {
				return Ok(progress.nothing_to_apply());
			}
			let has_more = plan.has_more();
			let captured = plan.captured_len();
			Datastore::ensure_not_cancelled(canceller)?;

			let txn = self.transaction(Write).await?;
			let ix = match compaction_write_guard(&txn, ikb).await {
				Ok(CompactionWriteGuard::Live(ix)) => ix,
				Ok(CompactionWriteGuard::Retired) => {
					let _ = txn.cancel().await;
					return Ok(progress.nothing_to_apply());
				}
				Ok(CompactionWriteGuard::Fenced) => {
					let _ = txn.cancel().await;
					return Ok(CompactionPassOutcome::Fenced);
				}
				Err(e) => {
					let _ = txn.cancel().await;
					if progress
						.retry(
							&e,
							format!(
								"Retryable conflict guarding full-text compaction for {ikb}, retrying"
							),
						)
						.await
					{
						continue;
					}
					return Err(e);
				}
			};
			let Index::FullText(p) = &ix.index else {
				let _ = txn.cancel().await;
				return Ok(progress.nothing_to_apply());
			};
			let res = IndexOperation::apply_fulltext_compaction(
				self.ctx.get_index_stores(),
				ikb,
				&txn,
				p,
				&self.ctx.config.idx.file_allowlist,
				ix.format_version,
				plan,
			)
			.await;
			match res {
				Ok(CompactionApply::Applied) => {
					if let Err(e) = Datastore::ensure_not_cancelled(canceller) {
						let _ = txn.cancel().await;
						return Err(e);
					}
					#[cfg(test)]
					if let Err(e) = maybe_inject_non_retryable_error(
						NonRetryableErrorSite::FullTextCompaction,
						self.node_id,
					) {
						let _ = txn.cancel().await;
						return Err(e);
					}
					#[cfg(test)]
					if let Err(e) = maybe_inject_retryable_conflict(
						RetryableConflictSite::FullTextCompaction,
						self.node_id,
					) {
						let _ = txn.cancel().await;
						if progress
							.retry(
								&e,
								format!(
									"Retryable conflict committing full-text compaction for {ikb}, retrying"
								),
							)
							.await
						{
							continue;
						}
						return Err(e);
					}
					if let Err(e) = txn.commit().await {
						let _ = txn.cancel().await;
						if progress
							.retry(
								&e,
								format!(
									"Retryable conflict committing full-text compaction for {ikb}, retrying"
								),
							)
							.await
						{
							continue;
						}
						return Err(e);
					}
				}
				Ok(CompactionApply::LostRace) => {
					let _ = txn.cancel().await;
					if progress.retry_lost_race(ikb) {
						continue;
					}
					return Ok(CompactionPassOutcome::LostRace);
				}
				Ok(CompactionApply::Empty) => {
					let _ = txn.cancel().await;
					return Ok(progress.nothing_to_apply());
				}
				Err(e) => {
					let _ = txn.cancel().await;
					if progress
						.retry(
							&e,
							format!(
								"Retryable conflict applying full-text compaction for {ikb}, retrying"
							),
						)
						.await
					{
						continue;
					}
					return Err(e);
				}
			}
			let outcome = progress.record_applied(
				IndexCompactionPlanMode::Folded,
				captured,
				plan_started.elapsed(),
				has_more,
			);
			Datastore::ensure_not_cancelled(canceller)?;
			if let Some(outcome) = outcome {
				return Ok(outcome);
			}
		}
	}

	/// Runs count-index compaction as a read-plan followed by a guarded write.
	///
	/// The write phase deletes only keys captured in the plan, so concurrent
	/// `!iu` deltas are preserved and included by later reads/compactions.
	///
	/// A cancel this pass makes after a step fails, or after a plan turns out
	/// to have nothing to apply, ignores its own error: the step's error or the
	/// pass's outcome is what the pass acts on and reports, and the transaction
	/// it cancels has not committed, so a cancel that fails discards nothing
	/// that could apply.
	async fn process_count_compaction(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		lease: Option<&dyn PassLease>,
		progress: &mut CompactionPassProgress,
	) -> Result<CompactionPassOutcome> {
		loop {
			let plan_started = Instant::now();
			Datastore::ensure_not_cancelled(canceller)?;
			if lease.is_some_and(|lease| lease.is_lost()) {
				return Ok(CompactionPassOutcome::Budget);
			}
			let plan = {
				let txn = self.transaction(Read).await?;
				let res = IndexOperation::prepare_count_compaction(ikb, &txn).await;
				let _ = txn.cancel().await;
				res?
			};
			if !plan.has_work() {
				return Ok(progress.nothing_to_apply());
			}
			let has_more = plan.has_more();
			let captured = plan.captured_len();
			#[cfg(test)]
			crate::kvs::ds::test::before_count_compaction_apply(self.node_id).await;
			Datastore::ensure_not_cancelled(canceller)?;

			let txn = self.transaction(Write).await?;
			let ix = match compaction_write_guard(&txn, ikb).await {
				Ok(CompactionWriteGuard::Live(ix)) => ix,
				Ok(CompactionWriteGuard::Retired) => {
					let _ = txn.cancel().await;
					return Ok(progress.nothing_to_apply());
				}
				Ok(CompactionWriteGuard::Fenced) => {
					let _ = txn.cancel().await;
					return Ok(CompactionPassOutcome::Fenced);
				}
				Err(e) => {
					let _ = txn.cancel().await;
					if progress
						.retry(
							&e,
							format!(
								"Retryable conflict guarding count compaction for {ikb}, retrying"
							),
						)
						.await
					{
						continue;
					}
					return Err(e);
				}
			};
			if !matches!(&ix.index, Index::Count(_)) {
				let _ = txn.cancel().await;
				return Ok(progress.nothing_to_apply());
			}
			let res = IndexOperation::apply_count_compaction(ikb, &txn, plan).await;
			match res {
				Ok(CompactionApply::Applied) => {
					if let Err(e) = Datastore::ensure_not_cancelled(canceller) {
						let _ = txn.cancel().await;
						return Err(e);
					}
					#[cfg(test)]
					if let Err(e) = maybe_inject_retryable_conflict(
						RetryableConflictSite::CountCompaction,
						self.node_id,
					) {
						let _ = txn.cancel().await;
						if progress
							.retry(
								&e,
								format!(
									"Retryable conflict committing count compaction for {ikb}, retrying"
								),
							)
							.await
						{
							continue;
						}
						return Err(e);
					}
					if let Err(e) = txn.commit().await {
						let _ = txn.cancel().await;
						if progress
							.retry(
								&e,
								format!(
									"Retryable conflict committing count compaction for {ikb}, retrying"
								),
							)
							.await
						{
							continue;
						}
						return Err(e);
					}
				}
				Ok(CompactionApply::LostRace) => {
					let _ = txn.cancel().await;
					if progress.retry_lost_race(ikb) {
						continue;
					}
					return Ok(CompactionPassOutcome::LostRace);
				}
				Ok(CompactionApply::Empty) => {
					let _ = txn.cancel().await;
					return Ok(progress.nothing_to_apply());
				}
				Err(e) => {
					let _ = txn.cancel().await;
					if progress
						.retry(
							&e,
							format!(
								"Retryable conflict applying count compaction for {ikb}, retrying"
							),
						)
						.await
					{
						continue;
					}
					return Err(e);
				}
			}
			let outcome = progress.record_applied(
				IndexCompactionPlanMode::Folded,
				captured,
				plan_started.elapsed(),
				has_more,
			);
			Datastore::ensure_not_cancelled(canceller)?;
			if let Some(outcome) = outcome {
				return Ok(outcome);
			}
		}
	}
}

/// How often a wait for materialisation rechecks its query's cancellation and
/// its deadlines, none of which a running pass is told of.
const MATERIALISE_DONE_CHECK: Duration = Duration::from_millis(100);

/// Longest a wait for materialisation keeps retrying a pass that a live build
/// owner fences, from the first fenced pass in a row: the owner's lease.
///
/// The owner releases the index once its build is online and the statement
/// that started it has closed, so a fence normally lasts as long as another
/// statement stays open. One that outlasts the lease is an owner that may not
/// release at all, and the wait ends as it does at a deadline, leaving the
/// backlog to background compaction.
const MATERIALISE_FENCE_PATIENCE: Duration =
	Duration::from_secs(super::BUILD_OWNER_LEASE_SECS as u64);

/// Phase 2 of a blocking `DEFINE INDEX` or `REBUILD INDEX`: once its
/// transaction has committed and its index is online, wait for the index to be
/// materialised, by running this index's passes in the statement's place.
///
/// A passive wait would hang wherever no background compaction runs, so the
/// wait drives the passes itself, under the index's materialise lock. It ends:
///
/// - with success once a pass ends on an empty or a short plan ([`CompactionPassOutcome::Empty`],
///   [`CompactionPassOutcome::Short`]): the backlog the pass read is folded. Writes that commit
///   after the statement keep adding to the backlog, so the wait never waits for it to be empty.
/// - with success once the index has no readable definition
///   ([`CompactionPassOutcome::NotVisible`]): it was removed or replaced, and has nothing of this
///   statement's left to fold.
/// - with success at its deadline — the sooner of the query's own and the one its owner hands it —
///   with the backlog left queued. The index is online and correct, and merges its backlog at read
///   time.
/// - with success once a live build owner has fenced the index for [`MATERIALISE_FENCE_PATIENCE`],
///   with the backlog left queued.
/// - with [`DatastoreError::IndexMaterialiseWaitStopped`] when the query is cancelled, or a pass
///   fails on an error that is not a retryable conflict.
///
/// A pass that ends on its budget or on a lost race is followed by another at
/// once, as is one that fails on a retryable conflict: such a pass has
/// retried up to its own cap and queued a fresh request, and on a datastore
/// with no background task nothing else would fold the index. A fenced pass is
/// followed by another after [`super::BUILD_CLOSING_SLEEP`].
///
/// A pass in progress is never abandoned: at a deadline or a cancellation the
/// wait trips the pass's canceller and awaits it, and the pass stops at its
/// next plan boundary. A cancellation also stops a DiskANN frame in progress,
/// which then commits nothing and leaves its backlog queued. The committed
/// index is never undone, so the wait owes its commit nothing
/// ([`AfterCommitWait::compensates`]).
pub(crate) struct MaterialiseAfterCommit {
	/// The statement's context: its deadline and cancellation, and the place
	/// registration the statement lends its index's passes.
	ctx: FrozenContext,
	ikb: IndexKeyBase,
}

/// Why a wait for materialisation stopped polling.
/// Waits for `lock` until `check` ends the wait, asking it again after each
/// `slice`.
///
/// One acquisition stays queued across every check. The lock hands itself out
/// in the order its waiters queued, so a background pass that starts waiting
/// later does not get it first.
///
/// # Arguments
/// * `lock` - the index's materialise lock ([`Materialiser::lock`]).
/// * `slice` - how long to wait before the next check.
/// * `check` - `Some` once the wait is no longer wanted.
///
/// # Returns
/// The guard, or the first `Some` that `check` returns.
pub(super) async fn lock_unless<'a, T>(
	lock: &'a tokio::sync::Mutex<()>,
	mut slice: impl FnMut() -> Duration,
	mut check: impl FnMut() -> Option<T>,
) -> std::result::Result<tokio::sync::MutexGuard<'a, ()>, T> {
	let mut acquire = std::pin::pin!(lock.lock());
	loop {
		tokio::select! {
			biased;
			held = &mut acquire => return Ok(held),
			_ = sleep(slice()) => {
				if let Some(done) = check() {
					return Err(done);
				}
			}
		}
	}
}

enum WaitDone {
	/// A deadline passed, or the fence outlasted the wait's patience.
	Settled,
	/// The query was cancelled.
	Cancelled,
}

impl MaterialiseAfterCommit {
	/// The wait for index `ikb`, queued by the statement running in `ctx`.
	pub(crate) fn new(ctx: FrozenContext, ikb: IndexKeyBase) -> Self {
		Self {
			ctx,
			ikb,
		}
	}

	/// Whether the wait is over before its next pass: the query cancelled, or
	/// a deadline passed.
	fn done(&self, deadline: Option<AfterCommitDeadline>) -> Option<WaitDone> {
		match super::builder::query_ended(&self.ctx) {
			Some(Reason::Canceled) => return Some(WaitDone::Cancelled),
			Some(Reason::Timedout(_)) => return Some(WaitDone::Settled),
			None => {}
		}
		match deadline {
			Some(deadline) if Instant::now() >= deadline.at => Some(WaitDone::Settled),
			_ => None,
		}
	}

	/// How long to wait before checking [`Self::done`] again: a short slice,
	/// cut to whichever deadline comes first.
	fn slice(&self, deadline: Option<AfterCommitDeadline>) -> Duration {
		let mut slice = MATERIALISE_DONE_CHECK;
		if let Some(deadline) = deadline {
			slice = slice.min(deadline.at.saturating_duration_since(Instant::now()));
		}
		if let Some(left) = self.ctx.timeout() {
			slice = slice.min(left);
		}
		slice
	}

	fn stopped(cancelled: bool, reason: String) -> anyhow::Error {
		DatastoreError::IndexMaterialiseWaitStopped {
			cancelled,
			reason,
		}
		.into()
	}

	async fn run(self, deadline: Option<AfterCommitDeadline>) -> Result<()> {
		let settle = |done: WaitDone| match done {
			WaitDone::Settled => Ok(()),
			WaitDone::Cancelled => Err(Self::stopped(true, "the query was cancelled".to_string())),
		};
		// Tripped only when the query is cancelled: a deadline ends the wait at
		// a plan boundary, with the frame in progress left to commit.
		let abort = CancellationToken::new();
		let materialiser = Materialiser::new(&self.ctx)?
			.with_frame_abort(abort.clone())
			.with_trigger(IndexCompactionTrigger::AfterCommit);
		// The statement's own registration, where it lent one, so the pass's
		// parallel work counts as the statement's places.
		let places = match self.ctx.index_places() {
			Some(places) => places.clone(),
			None => crate::ctx::IndexPlaces::node(
				crate::exec::planner::placement::Policy::from_config(&self.ctx.config.exec),
			),
		};
		let lock = materialiser.lock(&self.ikb);
		let mut fenced_since: Option<Instant> = None;
		let mut leases_released = false;
		loop {
			if let Some(done) = self.done(deadline) {
				return settle(done);
			}
			// Wait for any pass this process is running over the index to end.
			let held =
				match lock_unless(&lock, || self.slice(deadline), || self.done(deadline)).await {
					Ok(held) => held,
					Err(done) => return settle(done),
				};
			let canceller = CancellationToken::new();
			let report = {
				let mut pass = std::pin::pin!(
					materialiser.run_pass_held(&self.ikb, &canceller, None, &places)
				);
				loop {
					tokio::select! {
						report = &mut pass => break report,
						_ = sleep(self.slice(deadline)), if !canceller.is_cancelled() => {
							match self.done(deadline) {
								Some(WaitDone::Cancelled) => {
									abort.cancel();
									canceller.cancel();
								}
								Some(WaitDone::Settled) => canceller.cancel(),
								None => {}
							}
						}
					}
				}
			};
			drop(held);
			match report.result {
				Ok(CompactionPassOutcome::Empty | CompactionPassOutcome::Short) => {
					// On a store other datastores write to, a shard this node wrote into is
					// covered by its writer lease, and no node steps a guard under a lease, so
					// the drained guard would read `NonEmpty` until the lease went idle. The
					// backlog is folded, so the leases are released and the next pass steps
					// the guards.
					if !leases_released && materialiser.release_writer_leases(&self.ikb).await? > 0
					{
						leases_released = true;
						continue;
					}
					return Ok(());
				}
				Ok(CompactionPassOutcome::NotVisible) => {
					debug!(target: TARGET, "Materialise wait for {:?} found no definition to fold through, leaving its backlog queued", self.ikb);
					return Ok(());
				}
				Ok(CompactionPassOutcome::Budget | CompactionPassOutcome::LostRace) => {
					fenced_since = None;
				}
				Ok(CompactionPassOutcome::Fenced) => {
					let since = *fenced_since.get_or_insert_with(Instant::now);
					if since.elapsed() >= MATERIALISE_FENCE_PATIENCE {
						debug!(target: TARGET, "Materialise wait for {:?} stayed fenced by a live build owner, leaving its backlog queued", self.ikb);
						return Ok(());
					}
					sleep(super::BUILD_CLOSING_SLEEP.min(self.slice(deadline))).await;
				}
				// The canceller is this wait's own, tripped only once the wait
				// is over; the check at the top of the loop says how.
				Err(_) if canceller.is_cancelled() => {}
				Err(e) if is_retryable_transaction_conflict(&e) => {
					fenced_since = None;
				}
				Err(e) => return Err(Self::stopped(false, e.to_string())),
			}
		}
	}
}

impl AfterCommitWait for MaterialiseAfterCommit {
	fn wait(
		self: Box<Self>,
		deadline: Option<AfterCommitDeadline>,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> {
		Box::pin((*self).run(deadline))
	}

	fn compensates(&self) -> bool {
		false
	}
}
