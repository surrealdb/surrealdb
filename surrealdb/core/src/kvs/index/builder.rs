use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, ensure};
use chrono::Utc;
use futures::channel::oneshot::{Receiver, Sender, channel};
use surrealdb_datastore::close::{CommitAction, RollbackAction};
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;
use web_time::Instant;

use super::state::{
	build_owner_expired, delete_stale_build_queues, durable_index_error_reason,
	durable_report_count, is_condition_not_met, report_status_from_phase,
};
use super::{
	AcquiredBuild, BUILD_CLOSING_SLEEP, BuildGeneration, IndexBuildPhase, IndexBuildReportStatus,
	IndexBuildState, IndexBuilding, build_abort_deadline,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, Index, IndexDefinition, IndexId, NamespaceId, TableId};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::index::IndexOperation;
use crate::key::schema::{IdxRoot, RecordKey, RecordPrefix};
use crate::key::{KVKey, KVKeyDecode, Resumable};
use crate::kvs::sequences::Sequences;
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::kvs::{
	DatastoreError, Direction, INDEXING_BATCH_MAX_BYTES, INDEXING_BATCH_SIZE,
	INDEXING_PROBE_BATCH_SIZE, Transaction, TransactionFactory, TransactionType,
	is_retryable_transaction_conflict, is_shutdown_error,
};
/// How long to wait before retrying a conflicting cleanup of an uncommitted
/// index build. Matches the reservation-release pause it runs beside.
const UNCOMMITTED_BUILD_CLEANUP_RETRY_SLEEP: Duration = Duration::from_millis(100);

use crate::mem::ALLOC;
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Process-local key used only to deduplicate active builder tasks.
pub(super) type SharedIndexKey = Arc<IndexKey>;

/// Whether an error reports that the build crossed the process memory
/// threshold (see [`Building::is_beyond_threshold`]).
///
/// Memory pressure is load-transient: the interrupted build is safe to retry
/// later — typically after the resume scan re-adopts it once the owner lease
/// expires, when the pressure has receded or the process was restarted with
/// more memory — so it must not be recorded as a permanent build failure.
fn is_memory_threshold_error(err: &anyhow::Error) -> bool {
	matches!(err.downcast_ref::<DatastoreError>(), Some(DatastoreError::QueryBeyondMemoryThreshold))
}

/// Probe the initial-batch commit injection sites, so tests can fail the
/// commit with a generic non-retryable error, with the shutdown-class error
/// the storage engines return during graceful shutdown, or with the memory
/// threshold error.
#[cfg(test)]
fn maybe_inject_initial_batch_commit_error(node_id: Uuid) -> Result<()> {
	maybe_inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchCommit,
		node_id,
	)?;
	maybe_inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchShutdown,
		node_id,
	)?;
	maybe_inject_non_retryable_error(
		NonRetryableErrorSite::ConcurrentIndexInitialBatchMemoryThreshold,
		node_id,
	)
}

#[derive(Hash, PartialEq, Eq)]
pub(super) struct IndexKey {
	pub(super) ns: NamespaceId,
	pub(super) db: DatabaseId,
	pub(super) tb: TableName,
	pub(super) ix: IndexId,
}

impl IndexKey {
	pub(super) fn new(ns: NamespaceId, db: DatabaseId, tb: &TableName, ix: IndexId) -> Self {
		Self {
			ns,
			db,
			tb: tb.to_owned(),
			ix,
		}
	}
}

/// Process-local launcher for durable index builds.
///
/// The active-builder map is not the source of truth for build status. It only
/// tracks builder tasks running in this process so duplicate local tasks can be
/// rejected and removal can signal abort. Durable `!bs` state decides
/// cluster-wide ownership, planner visibility, and user-facing status.
#[derive(Clone)]
pub(crate) struct IndexBuilder {
	pub(super) tf: TransactionFactory,
	pub(super) indexes: Arc<RwLock<HashMap<SharedIndexKey, IndexBuilding>>>,
}

enum BuildStart {
	Started,
	RemoteOwner(IndexBuilding),
}

/// Document write data needed by the index builder.
///
/// The mutation can either be indexed immediately or written to the durable
/// distributed queue. Keeping the values and COUNT predicate result together
/// avoids re-evaluating write conditions during asynchronous replay.
pub(crate) struct IndexMutation<'a> {
	/// Values currently present in the index before the user write.
	pub(crate) old_values: Option<Vec<Value>>,
	/// Values that should be present in the index after the user write.
	pub(crate) new_values: Option<Vec<Value>>,
	/// Record whose index entries are changing.
	pub(crate) rid: &'a RecordId,
	/// Cached `(old_matches, new_matches)` for conditional COUNT indexes.
	pub(crate) count_cond_match: Option<(bool, bool)>,
}

impl IndexBuilder {
	pub(in crate::kvs) fn new(tf: TransactionFactory) -> Self {
		Self {
			tf,
			indexes: Default::default(),
		}
	}

	pub(crate) fn transaction_factory(&self) -> TransactionFactory {
		self.tf.clone()
	}

	/// Whether any local builder task is still running.
	///
	/// Used by the stalled-build resume scan to serialize recovery: while a
	/// build is active on this node, further adoptions are deferred to a
	/// later scan pass so concurrent initial scans cannot multiply the
	/// node's memory and CPU footprint.
	pub(crate) async fn has_unfinished_build(&self) -> bool {
		self.indexes.read().await.values().any(|building| !building.is_finished())
	}

	#[allow(clippy::too_many_arguments)]
	async fn start_building(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		ix_key: SharedIndexKey,
		sdr: Option<Sender<Result<()>>>,
	) -> Result<BuildStart> {
		let building = Arc::new(Building::new(ctx, self.tf.clone(), opt, tb, ix, ix_key)?);
		let acquired = match building.acquire_build_state().await {
			Ok(Some(acquired)) => acquired,
			Ok(None) => return Ok(BuildStart::RemoteOwner(building)),
			Err(err) => return Err(err),
		};
		self.start_acquired_building(Arc::clone(&building), acquired, sdr).await?;
		Ok(BuildStart::Started)
	}

	async fn start_acquired_building(
		&self,
		building: IndexBuilding,
		acquired: AcquiredBuild,
		sdr: Option<Sender<Result<()>>>,
	) -> Result<()> {
		{
			let mut indexes = self.indexes.write().await;
			if let Some(existing) = indexes.get(&building.ix_key) {
				ensure!(
					existing.is_finished(),
					DatastoreError::IndexAlreadyBuilding {
						name: building.ix.name.to_string(),
					}
				);
			}
			indexes.insert(Arc::clone(&building.ix_key), Arc::clone(&building));
		}
		let b = Arc::clone(&building);
		// Created before the spawn and moved in, so a task the executor drops
		// without ever polling it still marks the build finished. Waiters in
		// `Building::wait_finished` and the `is_finished` checks above would
		// otherwise never see a build that produced no writes at all.
		//
		// The `drop(guard)` below is what makes `async move` capture this, and it
		// also has to stay where it is: it must run after the build's last durable
		// write, so a waiter that sees the flag knows no further write is coming,
		// and before the result is sent, so a blocking `DEFINE INDEX` that has
		// returned cannot be rejected as `IndexAlreadyBuilding`. Letting the guard
		// fall out of scope instead would drop it here, at the end of this
		// function, and report every build finished before it had begun.
		let guard = BuildingFinishGuard(Arc::clone(&building));
		spawn(async move {
			let r = b.run_acquired(acquired).await;
			let generation = b.build_generation.load(Ordering::Acquire);
			if let Err(err) = &r {
				// Shutdown and memory-threshold failures are transient: the
				// storage engine went away under this builder, or the process
				// was under memory pressure — the build itself did not fail.
				// The durable state is deliberately left in
				// `Building`/`Closing` with its last committed checkpoint:
				// once the owner lease expires, the periodic resume scan (or
				// a blocking statement takeover) continues the build from
				// there. Recording a durable `Error` here would instead stop
				// the build permanently and fail every subsequent write to
				// the table on admission.
				if is_shutdown_error(err) {
					info!(
						index = %b.ix.name,
						table = %b.ix.table_name,
						"index build interrupted by datastore shutdown; \
						 it will resume after restart"
					);
				} else if is_memory_threshold_error(err) {
					warn!(
						index = %b.ix.name,
						table = %b.ix.table_name,
						"index build interrupted by the memory threshold; \
						 it will resume from its checkpoint once the owner \
						 lease expires"
					);
				} else if generation != 0 {
					let _ = b.mark_durable_error(generation, err.to_string()).await;
				}
			} else if b.aborted.load(Ordering::Acquire) && generation != 0 {
				let _ = b.mark_durable_aborted(generation).await;
			}
			// Publishes `finished` after the last durable write and before the
			// result is sent; it is also the only reason the guard is captured by
			// this future at all. See where the guard is constructed.
			drop(guard);
			if let Some(s) = sdr
				&& s.send(r).is_err()
			{
				warn!("Failed to send index building result to the consumer");
			}
		});
		Ok(())
	}

	/// Wait for a remote owner to finish a blocking build.
	///
	/// A blocking `DEFINE INDEX` or `REBUILD INDEX` must not return simply
	/// because another node already owns the durable generation. Once this path
	/// has observed a remote active build, it only waits for that generation to
	/// become `Online`/`Error` or takes over that same generation after the owner
	/// lease expires. It must not create a fresh generation after the remote
	/// owner finishes, because the blocking statement is waiting for that work.
	async fn wait_for_remote_building(
		&self,
		ctx: &FrozenContext,
		building: IndexBuilding,
	) -> Result<()> {
		loop {
			if let Some(reason) = ctx.done(true)? {
				return Err(Error::from(reason).into());
			}
			let Some(state) = building.read_durable_build_state().await? else {
				return Err(DatastoreError::IndexingBuildingCancelled {
					reason: format!("Index {} build state no longer exists", building.ix.name),
				}
				.into());
			};
			match state.phase {
				IndexBuildPhase::Online => return Ok(()),
				IndexBuildPhase::Error => {
					return Err(DatastoreError::IndexingBuildingCancelled {
						reason: format!(
							"{}. Run `REBUILD INDEX {} ON {}` to retry the build",
							durable_index_error_reason(&building.ix, &state),
							building.ix.name,
							building.ix.table_name
						),
					}
					.into());
				}
				IndexBuildPhase::Building | IndexBuildPhase::Closing => {
					if build_owner_expired(&state, Utc::now())
						&& let Some(acquired) = building.takeover_expired_build_state().await?
					{
						let (s, r) = channel();
						self.start_acquired_building(Arc::clone(&building), acquired, Some(s))
							.await?;
						return r.await.map_err(|_| DatastoreError::IndexingBuildingCancelled {
							reason: "Channel shutdown".to_string(),
						})?;
					}
					sleep(BUILD_CLOSING_SLEEP).await;
				}
			}
		}
	}

	/// Start a build task if this node can acquire durable ownership.
	///
	/// Non-blocking callers return immediately when another node owns a fresh
	/// lease. Blocking callers wait for the durable generation to become
	/// queryable or failed, taking over the same generation if the remote owner
	/// lease expires.
	pub(crate) async fn build(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		blocking: bool,
	) -> Result<Option<Receiver<Result<()>>>> {
		expect_not_prepare_remove(&ix)?;
		let (ns, db) = ctx.expect_ns_db_ids(&opt).await?;
		let key = Arc::new(IndexKey::new(ns, db, &ix.table_name.clone(), ix.index_id));
		let (rcv, sdr) = if blocking {
			let (s, r) = channel();
			(Some(r), Some(s))
		} else {
			(None, None)
		};
		if let Some(existing) = self.indexes.read().await.get(&key) {
			ensure!(
				existing.is_finished(),
				DatastoreError::IndexAlreadyBuilding {
					name: ix.name.to_string(),
				}
			);
		}
		match self.start_building(ctx, opt, tb, ix, key, sdr).await? {
			BuildStart::Started => Ok(rcv),
			BuildStart::RemoteOwner(building) if blocking => {
				self.wait_for_remote_building(ctx, building).await?;
				Ok(None)
			}
			BuildStart::RemoteOwner(_) => Ok(None),
		}
	}

	/// Resume an index build left unfinished by a crashed or expired owner.
	///
	/// A `CONCURRENTLY` build is fire-and-forget: the initiating statement
	/// returns as soon as the builder task is spawned. If the owning node then
	/// dies mid-build, nothing ever waits on that generation again, so the
	/// durable `!bs` state is stranded in `Building`/`Closing` and the index
	/// reports `status: indexing` with a frozen counter indefinitely. This
	/// performs the same expired-lease takeover the blocking path performs in
	/// [`Self::wait_for_remote_building`], but proactively, driven by the
	/// periodic resume scan rather than by a statement that is waiting.
	///
	/// Returns `Ok(true)` if this call adopted the generation and spawned a
	/// resume task. It is a no-op (`Ok(false)`) when the durable state is
	/// missing, already `Online`/`Error`, still covered by a live owner lease,
	/// or already being built by a task in this process, so it is safe to call
	/// for every index on every scan. The takeover is CAS-guarded, so racing
	/// scans on other cluster nodes resolve to a single winner.
	pub(crate) async fn resume_stalled(
		&self,
		ctx: &FrozenContext,
		opt: Options,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableId,
		ix: Arc<IndexDefinition>,
	) -> Result<bool> {
		let key = Arc::new(IndexKey::new(ns, db, &ix.table_name.clone(), ix.index_id));
		// Skip if a builder task for this index is already running locally.
		if let Some(existing) = self.indexes.read().await.get(&key)
			&& !existing.is_finished()
		{
			return Ok(false);
		}
		let building = Arc::new(Building::new(ctx, self.tf.clone(), opt, tb, ix, key)?);
		// Cheap read-only pre-check: only an unfinished build whose owner lease
		// has expired is eligible. Healthy indexes and live builds bail out here
		// without opening a write transaction.
		let Some(state) = building.read_durable_build_state().await? else {
			return Ok(false);
		};
		if !matches!(state.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			|| !build_owner_expired(&state, Utc::now())
		{
			return Ok(false);
		}
		// Claim the expired generation and spawn the resume task. A no-op result
		// (`None`) means another scanner or a concurrent statement won the race.
		match building.takeover_expired_build_state().await? {
			Some(acquired) => {
				self.start_acquired_building(building, acquired, None).await?;
				Ok(true)
			}
			None => Ok(false),
		}
	}
}

pub(super) struct Building {
	/// Context used during the build.
	pub(super) ctx: FrozenContext,
	/// Fencing token for this concrete builder task.
	pub(super) owner: Uuid,
	/// Options used during the build.
	pub(super) opt: Options,
	/// Table id being indexed.
	pub(super) tb: TableId,
	/// Base key for both queryable index data and durable build metadata.
	pub(super) ikb: IndexKeyBase,
	/// Transaction factory for new transactions.
	pub(super) tf: TransactionFactory,
	/// Index definition being built.
	pub(super) ix: Arc<IndexDefinition>,
	/// Index key (namespace/db/table/index ids).
	pub(super) ix_key: SharedIndexKey,
	/// Durable generation currently owned by this builder; zero before acquire.
	pub(super) build_generation: AtomicU64,
	/// Abort flag for the build process.
	pub(super) aborted: AtomicBool,
	/// Set when the spawned task exits so a later local build can start.
	pub(super) finished: AtomicBool,
	/// Wakes [`Building::wait_finished`] once `finished` is set. Private because
	/// the two must move together: a notification without the flag is a lost
	/// wake-up for every waiter that has not registered yet.
	finished_notify: Notify,
}

impl Building {
	pub(super) fn new(
		ctx: &FrozenContext,
		tf: TransactionFactory,
		opt: Options,
		tb: TableId,
		ix: Arc<IndexDefinition>,
		ix_key: SharedIndexKey,
	) -> Result<Self> {
		let ikb = IndexKeyBase::new(ix_key.ns, ix_key.db, ix.table_name.clone(), ix.index_id);
		Ok(Self {
			ctx: Context::new_concurrent(ctx).freeze(),
			owner: Uuid::now_v7(),
			opt,
			tb,
			ikb,
			tf,
			ix,
			ix_key,
			build_generation: AtomicU64::new(0),
			aborted: AtomicBool::new(false),
			finished: AtomicBool::new(false),
			finished_notify: Notify::new(),
		})
	}

	/// Acquire durable ownership for this build.
	///
	/// A fresh build creates the next generation. If another node is already in
	/// `Building` or `Closing`, this returns `None` while the owner lease is
	/// fresh. Once the lease expires, this builder takes over the same
	/// generation and resumes from the persisted phase instead of starting a new
	/// scan.
	pub(super) async fn acquire_build_state(&self) -> Result<Option<AcquiredBuild>> {
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let state_key = self.ikb.new_bs_key();
			let existing = tx.get_key(&state_key, None).await?;
			if let Some(current) = existing.as_ref()
				&& matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				if !build_owner_expired(current, Utc::now()) {
					tx.cancel().await?;
					return Ok(None);
				}
				let mut next = current.clone();
				next.owner = Some(self.owner);
				next.error = None;
				next.report_status =
					next.report_status.or_else(|| Some(report_status_from_phase(next.phase)));
				let now = Utc::now();
				next.updated_at = now;
				next.owner_heartbeat_at = Some(now);
				let res = tx.put_compare_key(&state_key, &next, Some(current)).await;
				match res {
					Ok(()) => {
						if self
							.commit_and_retryable_conflict(
								&tx,
								"transient conflict acquiring build ownership, retrying",
							)
							.await?
						{
							continue;
						}
						self.build_generation.store(current.generation, Ordering::Release);
						return Ok(Some(AcquiredBuild {
							generation: current.generation,
							phase: current.phase,
							initial_complete: current.initial_complete,
							initial_count: durable_report_count(current.initial),
							updates_count: durable_report_count(current.updated),
							initial_cursor: current.initial_cursor.clone(),
						}));
					}
					Err(err) if is_condition_not_met(&err) => {
						let _ = tx.cancel().await;
						continue;
					}
					Err(err) => {
						let _ = tx.cancel().await;
						return Err(err);
					}
				}
			}
			// New-generation takeover. The next generation's state is
			// installed FIRST, in the same transaction that removes the
			// previous generation's `!bt` ticket counter: allocation
			// compare-and-swaps that counter, so committing this transaction
			// fences off any further old-generation admissions (builds in
			// `Error` admit like `Building`). Only after that fence can the
			// prior-generation reservation drain converge to a stable empty
			// state; draining before the flip would race a
			// writer that reserves between the drain and the state commit —
			// its queued mutation would be wiped while its main-table write
			// could land after the new initial scan had already passed the
			// record.
			tx.cancel().await?;
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let existing = tx.get_key(&state_key, None).await?;
			if let Some(current) = existing.as_ref()
				&& matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				tx.cancel().await?;
				continue;
			}

			let generation = existing.as_ref().map(|s| s.generation.saturating_add(1)).unwrap_or(1);
			let now = Utc::now();
			let state = IndexBuildState {
				generation,
				phase: IndexBuildPhase::Building,
				owner: Some(self.owner),
				next_ticket: 0,
				initial_complete: false,
				updated_at: now,
				owner_heartbeat_at: Some(now),
				error: None,
				report_status: Some(IndexBuildReportStatus::Started),
				initial: None,
				updated: None,
				pending: None,
				initial_cursor: None,
			};
			// Fence writers still admitting under the previous generation.
			// Removing its ticket counter with a read-then-write delete
			// conflicts with any concurrent allocation, so once this flip
			// commits no further old-generation reservation can be created —
			// which is what makes the prior-generation drain below converge on
			// a stable empty state. A blind delete would not conflict on
			// last-writer-wins backends.
			if let Some(previous) = existing.as_ref().map(|s| s.generation) {
				let previous_bt = self.ikb.new_bt_key(previous);
				if let Some(current) = tx.get_key(&previous_bt, None).await? {
					tx.del_compare_key(&previous_bt, Some(&current)).await?;
				}
			}
			// Every active generation owns a counter, so writer admission always
			// compare-and-swaps a key that is present.
			tx.set_key(&self.ikb.new_bt_key(generation), &0).await?;
			let res = tx.put_compare_key(&state_key, &state, existing.as_ref()).await;
			match res {
				Ok(()) => {
					if self
						.commit_and_retryable_conflict(
							&tx,
							"transient conflict acquiring build ownership, retrying",
						)
						.await?
					{
						continue;
					}
				}
				Err(err) if is_condition_not_met(&err) => {
					let _ = tx.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = tx.cancel().await;
					return Err(err);
				}
			}
			self.build_generation.store(generation, Ordering::Release);
			// Old-generation writers holding tickets either commit their
			// queue entries (their rows become visible before the initial
			// scan starts) or fail their admission fence on the generation
			// change; wait for the stragglers, then wipe the stale queues
			// they can no longer extend. A brand-new index (generation 1,
			// no prior durable state) has no prior generations, so skip the
			// extra transactions: they would only widen the window between
			// installing `!bs` and registering the local task, which a
			// racing cancel-path cleanup uses to abort the build.
			if generation > 1 {
				self.wait_for_prior_generation_reservations(generation).await?;
				self.wipe_stale_build_queues(generation).await?;
			}
			return Ok(Some(AcquiredBuild {
				generation,
				phase: IndexBuildPhase::Building,
				initial_complete: false,
				initial_count: 0,
				updates_count: 0,
				initial_cursor: None,
			}));
		}
	}

	/// Delete the queues of every generation below `below`, in its own
	/// retry-looped transaction.
	///
	/// Runs after [`Self::wait_for_prior_generation_reservations`], so no
	/// old-generation writer can re-create the deleted entries. Idempotent: a
	/// crash in between leaves only unreferenced keys, and the restarting
	/// initial scan re-runs both steps.
	async fn wipe_stale_build_queues(&self, below: BuildGeneration) -> Result<()> {
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			if let Err(err) = delete_stale_build_queues(&tx, &self.ikb, below).await {
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict wiping stale build queues, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
			match tx.commit().await {
				Ok(()) => return Ok(()),
				Err(err) => {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict wiping stale build queues, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
			}
		}
	}

	/// Read the durable build-state record without changing ownership.
	async fn read_durable_build_state(&self) -> Result<Option<IndexBuildState>> {
		let tx = self.new_read_tx().await?;
		let state = catch!(tx, tx.get_key(&self.ikb.new_bs_key(), None).await);
		tx.cancel().await?;
		Ok(state)
	}

	/// Take over an existing active generation after its owner lease expires.
	///
	/// This is intentionally narrower than `acquire_build_state`: it never
	/// creates a fresh generation. Blocking callers use it only after they have
	/// observed a remote active build, so racing with a remote completion must
	/// resolve to "wait completed" rather than "start a replacement build".
	async fn takeover_expired_build_state(&self) -> Result<Option<AcquiredBuild>> {
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let state_key = self.ikb.new_bs_key();
			let Some(current) = tx.get_key(&state_key, None).await? else {
				tx.cancel().await?;
				return Ok(None);
			};
			if !matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
				|| !build_owner_expired(&current, Utc::now())
			{
				tx.cancel().await?;
				return Ok(None);
			}
			let mut next = current.clone();
			next.owner = Some(self.owner);
			next.error = None;
			next.report_status =
				next.report_status.or_else(|| Some(report_status_from_phase(next.phase)));
			let now = Utc::now();
			next.updated_at = now;
			next.owner_heartbeat_at = Some(now);
			let res = tx.put_compare_key(&state_key, &next, Some(&current)).await;
			match res {
				Ok(()) => {
					if self
						.commit_and_retryable_conflict(
							&tx,
							"transient conflict taking over build ownership, retrying",
						)
						.await?
					{
						continue;
					}
					self.build_generation.store(current.generation, Ordering::Release);
					return Ok(Some(AcquiredBuild {
						generation: current.generation,
						phase: current.phase,
						initial_complete: current.initial_complete,
						initial_count: durable_report_count(current.initial),
						updates_count: durable_report_count(current.updated),
						initial_cursor: current.initial_cursor.clone(),
					}));
				}
				Err(err) if is_condition_not_met(&err) => {
					let _ = tx.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = tx.cancel().await;
					return Err(err);
				}
			}
		}
	}

	/// CAS-update build state only if this builder still owns the generation.
	///
	/// This is the durable fencing point for state transitions. A builder that
	/// loses ownership stops before it can publish `Online` for work completed by
	/// another owner.
	async fn update_owned_build_state<F>(
		&self,
		generation: BuildGeneration,
		update: F,
	) -> Result<IndexBuildState>
	where
		F: FnMut(&mut IndexBuildState),
	{
		self.update_owned_build_state_inner(generation, update, false).await
	}

	/// [`Self::update_owned_build_state`], additionally advancing the
	/// generation's ticket counter in the same transaction when
	/// `fence_ticket_allocation` is set.
	///
	/// Writer admission compare-and-swaps that key, so an allocation that has
	/// read the current phase but not yet committed conflicts with this
	/// transition and retries against the phase it publishes. The ticket it
	/// burns is never issued, which is harmless.
	async fn update_owned_build_state_inner<F>(
		&self,
		generation: BuildGeneration,
		mut update: F,
		fence_ticket_allocation: bool,
	) -> Result<IndexBuildState>
	where
		F: FnMut(&mut IndexBuildState),
	{
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let state_key = self.ikb.new_bs_key();
			let Some(current) = tx.get_key(&state_key, None).await? else {
				tx.cancel().await?;
				return Err(DatastoreError::CorruptedIndex(
					"Index build state is missing during state update",
				)
				.into());
			};
			if current.generation != generation || current.owner != Some(self.owner) {
				tx.cancel().await?;
				return Err(DatastoreError::IndexingBuildingCancelled {
					reason: format!("Index build ownership was lost for {}", self.ix.name),
				}
				.into());
			}
			let mut next = current.clone();
			update(&mut next);
			let now = Utc::now();
			next.updated_at = now;
			next.owner_heartbeat_at = if next.owner == Some(self.owner) {
				Some(now)
			} else {
				None
			};
			if fence_ticket_allocation {
				let bt = self.ikb.new_bt_key(generation);
				// A generation predating the counter has none; its admissions
				// still CAS `!bs`, so the state write below fences them.
				//
				// The counter is advanced rather than rewritten: some backends
				// validate a transaction by value rather than by version, and
				// there a write that stores the value it read is invisible to
				// a concurrent allocation, making the fence a silent no-op.
				// Burning a ticket costs nothing, since tickets are opaque
				// ordering tokens and nothing depends on them being contiguous.
				//
				// The compare reads this transaction's own view, so it cannot
				// fail; a conflict with a concurrent allocation surfaces at
				// commit instead, which is where the retry loop handles it.
				if let Some(ticket) = tx.get_key(&bt, None).await?
					&& let Err(err) =
						tx.put_compare_key(&bt, &ticket.saturating_add(1), Some(&ticket)).await
				{
					let _ = tx.cancel().await;
					return Err(err);
				}
			}
			let res = tx.put_compare_key(&state_key, &next, Some(&current)).await;
			match res {
				Ok(()) => {
					if self
						.commit_and_retryable_conflict(
							&tx,
							"transient conflict updating build state, retrying",
						)
						.await?
					{
						continue;
					}
					return Ok(next);
				}
				Err(err) if is_condition_not_met(&err) => {
					let _ = tx.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = tx.cancel().await;
					return Err(err);
				}
			}
		}
	}

	/// Replace the user-facing `INFO FOR INDEX` report fields on a state value.
	fn set_report(
		state: &mut IndexBuildState,
		status: IndexBuildReportStatus,
		initial: Option<usize>,
		pending: Option<usize>,
		updated: Option<usize>,
	) {
		state.report_status = Some(status);
		state.initial = initial.map(|v| v as u64);
		state.pending = pending.map(|v| v as u64);
		state.updated = updated.map(|v| v as u64);
		if status != IndexBuildReportStatus::Error {
			state.error = None;
		}
	}

	/// Persist a progress update without changing the durable lifecycle phase.
	pub(super) async fn mark_durable_report(
		&self,
		generation: BuildGeneration,
		status: IndexBuildReportStatus,
		initial: Option<usize>,
		pending: Option<usize>,
		updated: Option<usize>,
	) -> Result<()> {
		self.update_owned_build_state(generation, |state| {
			Self::set_report(state, status, initial, pending, updated);
		})
		.await?;
		Ok(())
	}

	/// Mark that the initial record scan has finished for this generation.
	pub(super) async fn mark_durable_initial_complete(
		&self,
		generation: BuildGeneration,
	) -> Result<()> {
		self.update_owned_build_state(generation, |state| {
			if state.phase == IndexBuildPhase::Building {
				state.initial_complete = true;
				state.initial_cursor = None;
				state.error = None;
			}
		})
		.await?;
		Ok(())
	}

	/// Fenced, transaction-local update of the durable build state.
	///
	/// Reads the state through `tx` (observing writes already staged in the
	/// same transaction, such as the ownership heartbeat), verifies this
	/// builder still owns the generation, applies `update`, and stages the
	/// CAS write so it commits atomically with the rest of the transaction.
	///
	/// Must run after [`Self::maintain_build_ownership`] in the same
	/// transaction, which has already fenced this builder's ownership.
	async fn update_build_state_in_tx<F>(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		update: F,
	) -> Result<()>
	where
		F: FnOnce(&mut IndexBuildState),
	{
		let state_key = self.ikb.new_bs_key();
		let Some(current) = tx.get_key(&state_key, None).await? else {
			return Err(DatastoreError::CorruptedIndex(
				"Index build state is missing during build-state update",
			)
			.into());
		};
		if current.generation != generation || current.owner != Some(self.owner) {
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index build ownership was lost for {}", self.ix.name),
			}
			.into());
		}
		let mut next = current.clone();
		update(&mut next);
		tx.put_compare_key(&state_key, &next, Some(&current)).await?;
		Ok(())
	}

	/// Persist the initial-scan continuation cursor inside a batch transaction.
	///
	/// The cursor and the `initial` counter commit atomically with the batch
	/// they describe, so the durable state never points into an uncommitted
	/// span. A takeover that finds a cursor resumes the scan right after it
	/// instead of wiping the partial index data and rescanning from the start.
	async fn checkpoint_initial_scan(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		cursor: &RecordIdKey,
		initial_count: usize,
	) -> Result<()> {
		self.update_build_state_in_tx(tx, generation, |state| {
			state.initial_cursor = Some(cursor.clone());
			state.initial = Some(initial_count as u64);
		})
		.await
	}

	/// Complete the initial scan inside the COUNT tail-pass transaction.
	///
	/// The tail pass baselines `!bp` old states past the last checkpoint and
	/// does not delete the markers it consumes, so it is not idempotent:
	/// completion must commit atomically with it. If durable state still said
	/// "incomplete, resume after cursor" once the tail baselines were
	/// committed, a takeover would re-enter the tail pass and double-count
	/// the same records. Non-COUNT builds have no tail pass and complete via
	/// [`Self::mark_durable_initial_complete`] instead.
	async fn complete_initial_scan(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		initial_count: usize,
	) -> Result<()> {
		self.update_build_state_in_tx(tx, generation, |state| {
			state.initial_complete = true;
			state.initial_cursor = None;
			state.initial = Some(initial_count as u64);
			state.error = None;
		})
		.await
	}

	/// Enter `Closing`, which blocks new admissions before the final drain.
	///
	/// The transition advances the generation's ticket counter in the same
	/// transaction. Allocation compare-and-swaps that counter, so an admission
	/// that read `Building` but has not yet committed conflicts here and
	/// retries, observing `Closing` and waiting instead of reserving. Without
	/// that fence a reservation could commit after `Closing` is durable, be
	/// missed by the drain that follows, and let the build publish `Online`
	/// without ever replaying the write. The counter is advanced rather than
	/// rewritten because a same-value write does not conflict on backends that
	/// validate by value.
	pub(super) async fn mark_durable_closing(&self, generation: BuildGeneration) -> Result<()> {
		self.update_owned_build_state_inner(
			generation,
			|state| {
				if state.phase == IndexBuildPhase::Building {
					state.phase = IndexBuildPhase::Closing;
					state.error = None;
					state.report_status = Some(IndexBuildReportStatus::Indexing);
				}
			},
			true,
		)
		.await?;
		Ok(())
	}

	/// Refuse to publish a generation whose tickets came from two allocators.
	///
	/// A generation that owns a `!bt` counter has `next_ticket` initialised to
	/// zero, and nothing in this version advances it — only the legacy branch
	/// of writer admission does, and that runs solely when the counter is
	/// absent. A non-zero value therefore proves that a node predating the
	/// counter allocated against this generation, from a sequence that also
	/// starts at zero and cannot conflict with `!bt`. The two can hand out the
	/// same ticket, in which case one writer's `!br` and `!bg` entries
	/// overwrite the other's and its mutation is missing from the queue this
	/// build has just replayed.
	///
	/// Publishing would leave a silently incomplete index reporting `ready`.
	/// Failing instead records a durable error naming the rebuild, which starts
	/// a fresh generation and rescans the table from the records themselves.
	///
	/// This cannot catch a generation that ran to completion entirely on a node
	/// without the counter, which has no way to know `!bt` exists.
	async fn ensure_single_ticket_allocator(&self, generation: BuildGeneration) -> Result<()> {
		let tx = self.new_read_tx().await?;
		let counter = catch!(tx, tx.get_key(&self.ikb.new_bt_key(generation), None).await);
		let state = catch!(tx, tx.get_key(&self.ikb.new_bs_key(), None).await);
		tx.cancel().await?;
		// A rotated generation means this builder has already lost the build;
		// `mark_durable_online` fails on its own fence. Reporting a version
		// skew here would send the operator after the wrong problem.
		let Some(state) = state.filter(|state| state.generation == generation) else {
			return Ok(());
		};
		if counter.is_some() && state.next_ticket != 0 {
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!(
					"Index {} was built while nodes of different versions allocated writer \
					 tickets for build generation {generation}, so queued writes may have been \
					 overwritten. Run `REBUILD INDEX {} ON {}` to rebuild it",
					self.ix.name, self.ix.name, self.ix.table_name
				),
			}
			.into());
		}
		Ok(())
	}

	/// Publish the index as queryable once all admitted work has been replayed.
	pub(super) async fn mark_durable_online(
		&self,
		generation: BuildGeneration,
		initial: usize,
		updated: usize,
	) -> Result<()> {
		self.update_owned_build_state(generation, |state| {
			state.phase = IndexBuildPhase::Online;
			state.owner = None;
			state.initial_complete = true;
			state.error = None;
			Self::set_report(
				state,
				IndexBuildReportStatus::Ready,
				Some(initial),
				Some(0),
				Some(updated),
			);
		})
		.await?;
		Ok(())
	}

	/// Publish a durable build error if this builder still owns the generation.
	async fn mark_durable_error(&self, generation: BuildGeneration, error: String) -> Result<()> {
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let state_key = self.ikb.new_bs_key();
			let Some(current) = tx.get_key(&state_key, None).await? else {
				tx.cancel().await?;
				return Ok(());
			};
			if current.generation != generation || current.owner != Some(self.owner) {
				tx.cancel().await?;
				return Ok(());
			}
			let mut next = current.clone();
			next.phase = IndexBuildPhase::Error;
			next.owner = None;
			next.owner_heartbeat_at = None;
			next.error = Some(error.clone());
			next.report_status = Some(IndexBuildReportStatus::Error);
			next.updated_at = Utc::now();
			let res = tx.put_compare_key(&state_key, &next, Some(&current)).await;
			match res {
				Ok(()) => {
					if self
						.commit_and_retryable_conflict(
							&tx,
							"transient conflict marking build error, retrying",
						)
						.await?
					{
						continue;
					}
					return Ok(());
				}
				Err(err) if is_condition_not_met(&err) => {
					let _ = tx.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = tx.cancel().await;
					return Err(err);
				}
			}
		}
	}

	/// Publish an aborted user-facing status if this builder still owns the generation.
	async fn mark_durable_aborted(&self, generation: BuildGeneration) -> Result<()> {
		self.update_owned_build_state(generation, |state| {
			state.phase = IndexBuildPhase::Error;
			state.owner = None;
			state.report_status = Some(IndexBuildReportStatus::Aborted);
			state.error = None;
		})
		.await?;
		Ok(())
	}

	/// Heartbeat the build owner inside an indexing transaction.
	///
	/// The heartbeat is written with the same transaction that applies index
	/// data. That makes lease renewal and batch visibility move together, and it
	/// prevents an expired old owner from continuing after a takeover has fenced
	/// it out.
	///
	/// Call this before other writes in the transaction so the durable-state read
	/// is independent from transaction-local range tombstones, while the
	/// heartbeat still commits atomically with the batch.
	pub(super) async fn maintain_build_ownership(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		allowed: &[IndexBuildPhase],
	) -> Result<()> {
		let state_key = self.ikb.new_bs_key();
		let Some(current) = tx.get_key(&state_key, None).await? else {
			return Err(DatastoreError::CorruptedIndex(
				"Index build state is missing during ownership heartbeat",
			)
			.into());
		};
		if current.generation != generation
			|| current.owner != Some(self.owner)
			|| !allowed.contains(&current.phase)
		{
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index build ownership was lost for {}", self.ix.name),
			}
			.into());
		}
		let mut next = current.clone();
		let now = Utc::now();
		next.updated_at = now;
		next.owner_heartbeat_at = Some(now);
		tx.put_compare_key(&state_key, &next, Some(&current)).await?;
		Ok(())
	}

	async fn retryable_conflict(&self, err: &anyhow::Error, action: &str) -> bool {
		if is_retryable_transaction_conflict(err) {
			debug!(
				target: "surrealdb::core::kvs::index",
				index = %self.ix.name,
				table = %self.ix.table_name,
				action,
				error = %err,
				"retryable conflict during concurrent index build, retrying"
			);
			sleep(Duration::from_millis(100)).await;
			true
		} else {
			false
		}
	}

	pub(super) async fn cancel_and_retryable_conflict(
		&self,
		tx: &Transaction,
		err: &anyhow::Error,
		action: &str,
	) -> bool {
		let _ = tx.cancel().await;
		self.retryable_conflict(err, action).await
	}

	pub(super) async fn commit_and_retryable_conflict(
		&self,
		tx: &Transaction,
		action: &str,
	) -> Result<bool> {
		match tx.commit().await {
			Ok(()) => Ok(false),
			Err(err) => {
				if self.cancel_and_retryable_conflict(tx, &err, action).await {
					Ok(true)
				} else {
					Err(err)
				}
			}
		}
	}

	pub(super) async fn new_read_tx(&self) -> Result<Transaction> {
		self.tf.transaction(TransactionType::Read, self.ctx.try_get_sequences()?.clone()).await
	}

	pub(super) async fn new_write_tx_ctx(&self) -> Result<FrozenContext> {
		let tx = self
			.tf
			.transaction(TransactionType::Write, self.ctx.try_get_sequences()?.clone())
			.await?
			.into();
		let mut ctx = Context::new_child(&self.ctx);
		ctx.set_transaction(tx);
		Ok(ctx.freeze())
	}

	/// Creates a child context backed by a read transaction for preparing compaction plans.
	pub(super) async fn new_read_tx_ctx(&self) -> Result<FrozenContext> {
		let tx = self
			.tf
			.transaction(TransactionType::Read, self.ctx.try_get_sequences()?.clone())
			.await?
			.into();
		let mut ctx = Context::new_child(&self.ctx);
		ctx.set_transaction(tx);
		Ok(ctx.freeze())
	}

	/// Evicts the process-local HNSW wrapper after a failed index-builder compaction write.
	async fn evict_cached_hnsw_index(&self) {
		if let Err(err) =
			self.ctx.get_index_stores().remove_hnsw_index(self.tb, self.ikb.clone()).await
		{
			warn!("Failed to evict HNSW index after index-builder compaction error: {err}");
		}
	}

	pub(super) async fn check_prepare_remove_with_tx(
		&self,
		last_prepare_remove_check: &mut Instant,
		tx: &Transaction,
	) -> Result<()> {
		if last_prepare_remove_check.elapsed() < Duration::from_secs(5) {
			return Ok(());
		};
		// Check the index still exists and has not been marked for removal.
		// We use get_tb_index (returns Option) instead of expect_tb_index because
		// this check runs on a separate read transaction. During a blocking DEFINE
		// INDEX, the index definition is only committed after indexing completes,
		// so this read transaction may not yet see it.
		// If the index is not found, we continue — the prepare_remove flag can only
		// be set by REMOVE INDEX, which runs in a separate transaction.
		if let Some(ix) = tx
			.get_tb_index(
				self.ix_key.ns,
				self.ix_key.db,
				&self.ix.table_name.clone(),
				&self.ix.name,
				None,
			)
			.await?
		{
			expect_not_prepare_remove(&ix)?;
		}
		*last_prepare_remove_check = Instant::now();
		Ok(())
	}

	pub(super) async fn check_prepare_remove(
		&self,
		last_prepare_remove_check: &mut Instant,
	) -> Result<()> {
		let tx = self.new_read_tx().await?;
		catch!(tx, self.check_prepare_remove_with_tx(last_prepare_remove_check, &tx).await);
		tx.cancel().await?;
		Ok(())
	}

	/// Confirm a post-`Online` builder compaction can still write this index.
	///
	/// Durable `!bs` state proves the compaction writer still owns this build
	/// generation. When the catalog entry is visible, it must also still point
	/// at the same non-retiring index definition.
	///
	/// Blocking `DEFINE INDEX` is the one valid case where this builder may not
	/// see the catalog entry yet: the statement is still waiting for the build
	/// before committing the schema definition. In that case, matching durable
	/// `Online` state is enough to continue.
	pub(super) async fn compaction_write_still_owns_index(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
	) -> Result<bool> {
		if generation == 0 {
			return Ok(false);
		}
		let Some(state) = tx.get_key(&self.ikb.new_bs_key(), None).await? else {
			return Ok(false);
		};
		if state.generation != generation || state.phase != IndexBuildPhase::Online {
			return Ok(false);
		}

		if let Some(ix) = tx
			.get_tb_index_by_id(
				self.ix_key.ns,
				self.ix_key.db,
				&self.ix_key.tb,
				self.ix_key.ix,
				None,
			)
			.await?
		{
			return Ok(ix.index_id == self.ix.index_id
				&& ix.name == self.ix.name
				&& !ix.prepare_remove);
		}

		Ok(true)
	}

	#[cfg(test)]
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(super) async fn run(&self) -> Result<()> {
		let Some(acquired) = self.acquire_build_state().await? else {
			return Ok(());
		};
		let generation = acquired.generation;
		let res = self.run_acquired(acquired).await;
		if res.is_ok() && self.aborted.load(Ordering::Acquire) {
			let _ = self.mark_durable_aborted(generation).await;
		}
		res
	}

	/// Execute a build after durable ownership has already been acquired.
	///
	/// Takeover from `Building` with an incomplete initial scan resumes the
	/// scan after the last per-batch checkpoint when one exists, and only
	/// restarts it (after cleaning index data for this generation) when the
	/// previous owner never committed a batch. Takeover from `Closing`, or
	/// from `Building` after `initial_complete`, skips the initial scan and
	/// only drains durable appendings and reservations.
	pub(super) async fn run_acquired(&self, acquired: AcquiredBuild) -> Result<()> {
		let mut last_prepare_remove_check = Instant::now();
		let generation = acquired.generation;
		let scanning_initial =
			acquired.phase == IndexBuildPhase::Building && !acquired.initial_complete;
		// Resume from the durable per-batch checkpoint when one exists: every
		// record up to the cursor was committed atomically with the cursor, so
		// the partial index data is consistent and does not need to be wiped.
		let resume_cursor = if scanning_initial {
			acquired.initial_cursor.clone()
		} else {
			None
		};
		let restarting_initial_scan = scanning_initial && resume_cursor.is_none();
		// A restarted incomplete scan discards previous progress because it first
		// cleans index data. Resumed and checkpoint-continued builds keep the
		// durable counters that have already been reported for this generation.
		let mut initial_count = if restarting_initial_scan {
			0
		} else {
			acquired.initial_count
		};
		let mut updates_count = if restarting_initial_scan {
			0
		} else {
			acquired.updates_count
		};

		if restarting_initial_scan {
			self.mark_durable_report(
				generation,
				IndexBuildReportStatus::Cleaning,
				None,
				None,
				None,
			)
			.await?;
			// A restarted scan can follow a crash between the generation flip
			// and the stale-queue wipe in `acquire_build_state`. Prior
			// generations' writers may then still hold live reservations
			// whose main-table writes must become visible before this scan
			// starts; re-run the drain and the wipe — both are cheap no-ops
			// when the previous owner already completed them.
			self.wait_for_prior_generation_reservations(generation).await?;
			self.wipe_stale_build_queues(generation).await?;
			loop {
				if self.is_aborted().await {
					return Ok(());
				}
				let ctx = self.new_write_tx_ctx().await?;
				let key = IdxRoot {
					ns: self.ix_key.ns,
					db: self.ix_key.db,
					tb: Cow::Borrowed(&self.ix_key.tb),
					ix: self.ix_key.ix,
				};
				let tx = ctx.tx();
				if let Err(err) = self
					.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
					.await
				{
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict maintaining build ownership, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
				if let Err(err) = tx.del_prefix_key(&key).await {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict while cleaning existing index data, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
				#[cfg(test)]
				if let Err(err) = maybe_inject_retryable_conflict(
					RetryableConflictSite::ConcurrentIndexInitialCleanup,
					self.ctx.node_id(),
				) {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict while cleaning existing index data, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
				match tx.commit().await {
					Ok(()) => break,
					Err(err) => {
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict while cleaning existing index data, retrying",
							)
							.await
						{
							continue;
						}
						return Err(err);
					}
				}
			}
		}
		if scanning_initial {
			// First pass: index every record, resuming immediately after the
			// checkpointed record when the previous owner committed batches.
			let mut range = RecordPrefix {
				ns: self.ix_key.ns,
				db: self.ix_key.db,
				tb: Cow::Borrowed(self.ikb.table()),
			}
			.range()?;
			if let Some(cursor) = &resume_cursor {
				// Resume at the smallest key strictly greater than the cursor
				// record's own key, so the checkpointed record is not re-indexed.
				let checkpoint = RecordKey {
					ns: self.ix_key.ns,
					db: self.ix_key.db,
					tb: Cow::Borrowed(self.ikb.table()),
					id: Cow::Borrowed(cursor),
				}
				.encode_key()?;
				range = range.resume_after(&checkpoint, Direction::Forward);
			}
			let mut next = Some(range);
			let mut v1_appending_sentinel = false;
			// On resume the COUNT primary-appending catch-up also continues from
			// the checkpoint: `!bp` markers at or before the cursor were baselined
			// atomically with the batch that covered them.
			let mut count_primary_cursor =
				matches!(self.ix.index, Index::Count(_)).then(|| resume_cursor.clone());
			// Batches are fetched by record count, so their memory footprint is
			// unbounded for large documents. The scan starts with a small probe
			// batch and then sizes each batch from the record sizes observed so
			// far, keeping a batch's raw record data around
			// `INDEXING_BATCH_MAX_BYTES` (small records keep using full
			// `INDEXING_BATCH_SIZE` batches). The checkpoint protocol is
			// per-batch and does not depend on a fixed batch size.
			let mut scan_batch_size = INDEXING_PROBE_BATCH_SIZE;
			// Set the initial status.
			self.mark_durable_report(
				generation,
				IndexBuildReportStatus::Indexing,
				Some(initial_count),
				Some(0),
				None,
			)
			.await?;

			while let Some(rng) = next {
				if self.is_aborted().await {
					return Ok(());
				}
				self.is_beyond_threshold(None)?;
				let batch = {
					let tx = self.new_read_tx().await?;
					// Check if the index has been marked for removal
					catch!(
						tx,
						self.check_prepare_remove_with_tx(&mut last_prepare_remove_check, &tx)
							.await
					);
					// Get the next batch of records.
					let res = catch!(
						tx,
						tx.batch_keys_vals_raw(rng.clone(), scan_batch_size, None).await
					);
					tx.cancel().await?;
					res
				};
				// Set the next scan range: a full page resumes just after the last
				// key it returned, while a short page ends the scan.
				next = batch
					.next
					.and(batch.result.last())
					.map(|(last, _)| rng.resume_after(last, Direction::Forward));
				// Check whether any records remain.
				if batch.result.is_empty() {
					// If not, initial indexing is complete.
					break;
				}
				// Size the next batch from the average record size seen in this
				// one, so the scan converges on the byte budget within one batch.
				{
					let bytes: usize = batch.result.iter().map(|(k, v)| k.len() + v.len()).sum();
					let avg = (bytes / batch.result.len()).max(1);
					scan_batch_size = (INDEXING_BATCH_MAX_BYTES / avg)
						.clamp(1, INDEXING_BATCH_SIZE as usize) as u32;
				}
				// Create a new context with a write transaction.
				{
					let values = batch.result;
					// Continuation checkpoint committed with this batch: the id
					// of the last record in the batch.
					let Some((last_key, _)) = values.last() else {
						// Unreachable: emptiness was checked above.
						break;
					};
					let batch_cursor = RecordKey::decode_key(last_key)?.id.into_owned();
					let indexed = loop {
						if self.is_aborted().await {
							return Ok(());
						}
						let ctx = self.new_write_tx_ctx().await?;
						let tx = ctx.tx();
						let saved_count_primary_cursor = count_primary_cursor.clone();
						if let Err(err) = self
							.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
							.await
						{
							count_primary_cursor = saved_count_primary_cursor;
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict maintaining build ownership, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
						// Index the batch.
						let indexed = match self
							.index_initial_batch(
								&ctx,
								&tx,
								&values,
								initial_count,
								&mut v1_appending_sentinel,
								&mut count_primary_cursor,
							)
							.await
						{
							Ok(indexed) => indexed,
							Err(err) => {
								count_primary_cursor = saved_count_primary_cursor;
								if self
									.cancel_and_retryable_conflict(
										&tx,
										&err,
										"transient conflict in initial index batch, retrying",
									)
									.await
								{
									continue;
								}
								return Err(err);
							}
						};
						// An abort observed inside `index_initial_batch` truncates
						// the batch, but `batch_cursor` still points at its last
						// record. Discard the transaction instead of committing a
						// checkpoint that covers records that were never indexed.
						// The abort flag is sticky, so this re-check cannot miss
						// a mid-batch abort.
						if self.is_aborted().await {
							tx.cancel().await?;
							return Ok(());
						}
						// Persist the continuation checkpoint atomically with the
						// batch it covers, so a takeover resumes the scan here
						// instead of wiping and rescanning from the start.
						if let Err(err) = self
							.checkpoint_initial_scan(
								&tx,
								generation,
								&batch_cursor,
								initial_count + indexed,
							)
							.await
						{
							count_primary_cursor = saved_count_primary_cursor;
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict checkpointing the initial scan, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
						#[cfg(test)]
						if let Err(err) = maybe_inject_retryable_conflict(
							RetryableConflictSite::ConcurrentIndexInitialBatch,
							self.ctx.node_id(),
						) {
							count_primary_cursor = saved_count_primary_cursor;
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict on initial index batch commit, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
						#[cfg(test)]
						if let Err(err) =
							maybe_inject_initial_batch_commit_error(self.ctx.node_id())
						{
							count_primary_cursor = saved_count_primary_cursor;
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict on initial index batch commit, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
						match tx.commit().await {
							Ok(()) => break indexed,
							Err(err) => {
								count_primary_cursor = saved_count_primary_cursor;
								if self
									.cancel_and_retryable_conflict(
										&tx,
										&err,
										"transient conflict on initial index batch commit, retrying",
									)
									.await
								{
									continue;
								}
								return Err(err);
							}
						}
					};
					initial_count += indexed;
					if !self.is_aborted().await {
						self.mark_durable_report(
							generation,
							IndexBuildReportStatus::Indexing,
							Some(initial_count),
							Some(0),
							None,
						)
						.await?;
					}
				}
			}
			if count_primary_cursor.is_some() {
				let indexed = loop {
					if self.is_aborted().await {
						return Ok(());
					}
					let ctx = self.new_write_tx_ctx().await?;
					let tx = ctx.tx();
					let saved_count_primary_cursor = count_primary_cursor.clone();
					if let Err(err) = self
						.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
						.await
					{
						count_primary_cursor = saved_count_primary_cursor;
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict maintaining build ownership, retrying",
							)
							.await
						{
							continue;
						}
						return Err(err);
					}
					let indexed = match self
						.index_remaining_count_primary_appendings(
							&ctx,
							&tx,
							&mut count_primary_cursor,
							initial_count,
						)
						.await
					{
						Ok(indexed) => indexed,
						Err(err) => {
							count_primary_cursor = saved_count_primary_cursor;
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict in initial count appending range, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
					};
					// An abort observed inside the tail pass truncates the
					// `!bp` catch-up. Discard the transaction instead of
					// committing a completion marker over baselines that were
					// never written.
					if self.is_aborted().await {
						tx.cancel().await?;
						return Ok(());
					}
					// The tail baselines are not idempotent, so the scan must
					// be marked complete atomically with them: a takeover that
					// still saw "incomplete, resume after cursor" would replay
					// the same `!bp` old states and double-count them.
					if let Err(err) =
						self.complete_initial_scan(&tx, generation, initial_count + indexed).await
					{
						count_primary_cursor = saved_count_primary_cursor;
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict completing the initial scan, retrying",
							)
							.await
						{
							continue;
						}
						return Err(err);
					}
					match tx.commit().await {
						Ok(()) => {
							#[cfg(test)]
							maybe_inject_non_retryable_error(
								NonRetryableErrorSite::ConcurrentIndexCountTailCommitted,
								self.ctx.node_id(),
							)?;
							break indexed;
						}
						Err(err) => {
							count_primary_cursor = saved_count_primary_cursor;
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict on initial count appending commit, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
					}
				};
				initial_count += indexed;
			} else {
				// Mark initial build as complete before entering the appending
				// phase. COUNT builds completed above, atomically with the
				// tail-pass transaction.
				self.mark_durable_initial_complete(generation).await?;
			}
		}
		// First replay pass: catch up with writes that were admitted while the
		// initial scan was running. The build is still in `Building`, so new
		// writers may continue to reserve tickets.
		self.mark_durable_report(
			generation,
			IndexBuildReportStatus::Indexing,
			Some(initial_count),
			Some(0),
			Some(updates_count),
		)
		.await?;
		self.index_appending_loop(
			initial_count,
			&mut updates_count,
			&mut last_prepare_remove_check,
		)
		.await?;
		if acquired.phase == IndexBuildPhase::Building {
			self.mark_durable_closing(generation).await?;
		}
		// Second replay pass: after `Closing`, no new admissions are created, but
		// writers that already reserved tickets may still be committing their
		// durable appendings.
		self.index_appending_loop(
			initial_count,
			&mut updates_count,
			&mut last_prepare_remove_check,
		)
		.await?;
		self.wait_for_durable_reservations(generation, &mut last_prepare_remove_check).await?;
		// Final replay pass: reservations have cleared, so any remaining queued
		// appendings are the last work that can exist before publishing `Online`.
		self.index_appending_loop(
			initial_count,
			&mut updates_count,
			&mut last_prepare_remove_check,
		)
		.await?;
		self.ensure_single_ticket_allocator(generation).await?;
		self.mark_durable_online(generation, initial_count, updates_count).await?;
		// Drain the table's durable pending doc-ID reclaim markers, best-effort:
		// it runs directly after the `Online` commit — before the fallible
		// compaction passes below can skip it — and an error must not fail a
		// build that already published successfully. The markers are durable, so
		// anything left behind is reclaimed by the next doc-ID index build.
		if let Err(err) = self.reclaim_deferred_doc_ids().await {
			warn!(
				index = %self.ix.name,
				table = %self.ix.table_name,
				error = %err,
				"deferred doc-ID reclaim sweep failed; leftover markers will be \
				 reclaimed by the next doc-ID index build on the table"
			);
		}
		self.compact_hnsw_pendings(&mut last_prepare_remove_check).await?;
		#[cfg(diskann)]
		self.compact_diskann_pendings(&mut last_prepare_remove_check).await?;
		Ok(())
	}

	/// Drains pending HNSW updates while a blocking `DEFINE INDEX` build is still running.
	async fn compact_hnsw_pendings(&self, last_prepare_remove_check: &mut Instant) -> Result<()> {
		let Index::Hnsw(p) = &self.ix.index else {
			return Ok(());
		};
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(None)?;
			self.check_prepare_remove(last_prepare_remove_check).await?;

			let plan = {
				let ctx = self.new_read_tx_ctx().await?;
				let tx = ctx.tx();
				let res = IndexOperation::prepare_hnsw_compaction(&ctx, &self.ikb).await;
				let cancel = tx.cancel().await;
				match res {
					Ok(plan) => {
						cancel?;
						plan
					}
					Err(err) => {
						let _ = cancel;
						return Err(err);
					}
				}
			};

			if !plan.has_work() {
				return Ok(());
			}
			let has_more = plan.has_more();

			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let generation = self.build_generation.load(Ordering::Acquire);
			if !self.compaction_write_still_owns_index(&tx, generation).await? {
				tx.cancel().await?;
				return Ok(());
			}
			let res = IndexOperation::apply_hnsw_compaction(
				&ctx,
				ctx.get_index_stores(),
				&self.ikb,
				p,
				plan,
			)
			.await;
			match res {
				Ok(true) => {
					if let Err(err) = tx.commit().await {
						self.evict_cached_hnsw_index().await;
						return Err(err);
					}
				}
				Ok(false) => {
					tx.cancel().await?;
					return Ok(());
				}
				Err(err) => {
					let _ = tx.cancel().await;
					self.evict_cached_hnsw_index().await;
					return Err(err);
				}
			}

			if !has_more {
				return Ok(());
			}
		}
	}

	#[cfg(diskann)]
	/// Drains pending DiskANN updates while a blocking `DEFINE INDEX` build is still running.
	async fn compact_diskann_pendings(
		&self,
		last_prepare_remove_check: &mut Instant,
	) -> Result<()> {
		let Index::DiskAnn(p) = &self.ix.index else {
			return Ok(());
		};
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(None)?;
			self.check_prepare_remove(last_prepare_remove_check).await?;

			let plan = {
				let ctx = self.new_read_tx_ctx().await?;
				let tx = ctx.tx();
				let res = IndexOperation::prepare_diskann_compaction(&ctx, &self.ikb).await;
				let cancel = tx.cancel().await;
				match res {
					Ok(plan) => {
						cancel?;
						plan
					}
					Err(err) => {
						let _ = cancel;
						return Err(err);
					}
				}
			};

			if !plan.requires_apply() {
				return Ok(());
			}
			let has_more = plan.has_more();

			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let generation = self.build_generation.load(Ordering::Acquire);
			if !self.compaction_write_still_owns_index(&tx, generation).await? {
				tx.cancel().await?;
				return Ok(());
			}
			// `apply_diskann_compaction` normally owns the transaction's
			// lifecycle (commits on success, cancels on apply failure while
			// holding the graph write lock — closing the #7318 race). A few
			// pre-apply paths inside `IndexOperation::apply_diskann_compaction`
			// (missing table or catalog lookup errors) can return without
			// finalizing the tx, so we add an idempotent safety net here:
			// cancel only if the tx is still open. Cancel on an already-closed
			// tx returns `TransactionFinished` and is harmlessly discarded.
			let res = IndexOperation::apply_diskann_compaction(
				&ctx,
				ctx.get_index_stores(),
				&self.ikb,
				p,
				plan,
			)
			.await;
			if !tx.closed() {
				let _ = tx.cancel().await;
			}
			match res {
				Ok(true) => {}
				Ok(false) => return Ok(()),
				Err(err) => return Err(err),
			}

			if !has_more {
				return Ok(());
			}
		}
	}

	/// Abort the current indexing process.
	pub(super) fn abort(&self) {
		// We use `Ordering::Relaxed` as the caller does not require synchronization.
		// We just want the current builder to eventually stop.
		self.aborted.store(true, Ordering::Relaxed);
	}

	/// Check if the indexing process should stop at the next check point.
	///
	/// Set by a user abort (`REMOVE INDEX`). A datastore shutdown does not use
	/// this: the commit coordinator's shutdown refuses the builder's commits
	/// before they apply, so the task exits on that error without any durable
	/// change and the build resumes after restart.
	pub(super) async fn is_aborted(&self) -> bool {
		// We use `Ordering::Relaxed` as there are no shared data accesses requiring
		// synchronization. This method is only called by the single thread building
		// the index.
		self.aborted.load(Ordering::Relaxed)
	}

	pub(super) fn is_beyond_threshold(&self, count: Option<usize>) -> Result<()> {
		if let Some(count) = count
			&& count % 100 != 0
		{
			return Ok(());
		}
		if ALLOC.is_beyond_threshold() {
			Err(anyhow::Error::new(DatastoreError::QueryBeyondMemoryThreshold))
		} else {
			Ok(())
		}
	}

	/// Whether the spawned task for this build has exited.
	///
	/// Loads `Acquire` to pair with the `Release` store in
	/// [`BuildingFinishGuard`]: a caller that observes `true` also observes
	/// everything the task did before it stopped, so this reads as "the builder
	/// has stopped writing" without further reasoning about ordering.
	pub(super) fn is_finished(&self) -> bool {
		self.finished.load(Ordering::Acquire)
	}

	/// Wait until the spawned task for this build has exited.
	///
	/// The task publishes its last durable write before it exits, so a caller
	/// that deletes this index's durable build state waits here first to make
	/// its delete the final write. Only a build reachable through
	/// [`IndexBuilder::indexes`] has a task; waiting on a [`Building`] that was
	/// never spawned never returns, so callers bound the wait with the drain's
	/// deadline.
	pub(super) async fn wait_finished(&self) {
		loop {
			// Register for the wake-up before re-checking the flag: the task can
			// finish between the two, and `notify_waiters` only wakes waiters
			// that are already registered.
			let notified = self.finished_notify.notified();
			tokio::pin!(notified);
			notified.as_mut().enable();
			if self.is_finished() {
				return;
			}
			notified.await;
		}
	}
}

struct BuildingFinishGuard(IndexBuilding);

impl Drop for BuildingFinishGuard {
	fn drop(&mut self) {
		// `Release` pairs with the `Acquire` load in `Building::wait_finished`,
		// so a waiter woken by the notification observes the flag as set.
		self.0.finished.store(true, Ordering::Release);
		self.0.finished_notify.notify_waiters();
	}
}

/// Rejects an index that is staged for removal.
///
/// A definition with `prepare_remove` set is in the two-phase removal window;
/// building or replaying against it would resurrect index state the removal
/// is about to purge.
fn expect_not_prepare_remove(ix: &IndexDefinition) -> anyhow::Result<()> {
	if ix.prepare_remove {
		Err(anyhow::Error::new(crate::kvs::DatastoreError::IndexingBuildingCancelled {
			reason: "Prepare remove.".to_string(),
		}))
	} else {
		Ok(())
	}
}

/// Stop a process-local index builder once a schema retirement has committed.
///
/// The durable side of retirement - deleting build state and the catalog entry -
/// is staged in the schema transaction. The builder map is process memory and is
/// not, so aborting before the commit would stop a build that a rollback then
/// leaves valid.
pub(crate) struct AbortLocalBuild {
	pub(crate) builder: IndexBuilder,
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) tb: TableName,
	pub(crate) ix: IndexId,
}

impl AbortLocalBuild {
	pub(crate) fn boxed(
		builder: IndexBuilder,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) -> Box<dyn CommitAction> {
		Box::new(Self {
			builder,
			ns,
			db,
			tb,
			ix,
		})
	}
}

impl CommitAction for AbortLocalBuild {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
		Box::pin(async move {
			if let Err(err) = self.builder.remove_index(self.ns, self.db, &self.tb, self.ix).await {
				warn!(
					"failed to abort local index builder after committed schema retirement: {err}"
				);
			}
		})
	}
}

/// Remove an index build whose catalog definition never committed.
///
/// `DEFINE INDEX` starts the builder while its schema transaction is still open,
/// so by the time that transaction is cancelled the builder may already have
/// committed build state and index data of its own, from separate transactions.
/// This deletes that provisional state, so a retry sees a clean slate.
///
/// The deletes go through the transaction's ordinary write methods and so count
/// against the write-cardinality guard, unlike the reservation release that runs
/// beside it. Six writes is far below any limit an operator would set, and a
/// cleanup that silently bypassed the guard would be the stranger choice.
pub(crate) struct CleanUncommittedBuild {
	pub(crate) builder: IndexBuilder,
	pub(crate) tf: TransactionFactory,
	pub(crate) sequences: Sequences,
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) tb: TableName,
	pub(crate) ix: IndexId,
}

impl CleanUncommittedBuild {
	pub(crate) fn boxed(
		builder: IndexBuilder,
		tf: TransactionFactory,
		sequences: Sequences,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableName,
		ix: IndexId,
	) -> Box<dyn RollbackAction> {
		Box::new(Self {
			builder,
			tf,
			sequences,
			ns,
			db,
			tb,
			ix,
		})
	}

	async fn cleanup_once(&self, abort_deadline: Instant) -> Result<()> {
		// Stop the local builder and wait for it to exit, so the deletes below are
		// the build's last writes: a builder write that lands after them re-creates
		// state nothing will ever collect, because the catalog never referenced this
		// index id, so no retirement and no resume scan can reach it. That same
		// unreachability is why waiting for the local task is enough on a cluster:
		// a remote node can only own a build it found through a committed catalog
		// entry, which this index id never had. Writer admission is fenced
		// separately, by the `!bt` counter range deleted alongside the rest.
		//
		// `abort_deadline` comes from the start of the close drain, so a schema
		// transaction that defined several indexes waits once rather than once per
		// index.
		self.builder
			.remove_index_and_wait(self.ns, self.db, &self.tb, self.ix, abort_deadline)
			.await;

		let tx = self.tf.transaction(TransactionType::Write, self.sequences.clone()).await?;
		let ikb = IndexKeyBase::new(self.ns, self.db, self.tb.clone(), self.ix);
		let index_prefix = IdxRoot {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(&self.tb),
			ix: self.ix,
		}
		.range()?;
		let result: Result<()> = async {
			tx.del_key(&ikb.new_bs_key()).await?;
			tx.delr(ikb.new_bg_all_generations_range()?).await?;
			tx.delr(ikb.new_bp_all_generations_range()?).await?;
			tx.delr(ikb.new_br_all_generations_range()?).await?;
			tx.delr(ikb.new_bt_all_generations_range()?).await?;
			tx.delr(index_prefix).await?;
			tx.commit_bare().await?;
			Ok(())
		}
		.await;
		if let Err(err) = result {
			let _ = tx.cancel_bare().await;
			return Err(err);
		}
		Ok(())
	}
}

impl RollbackAction for CleanUncommittedBuild {
	fn run(
		self: Box<Self>,
		drain_started_at: Instant,
	) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
		let abort_deadline = build_abort_deadline(drain_started_at);
		Box::pin(async move {
			loop {
				match self.cleanup_once(abort_deadline).await {
					Ok(()) => return Ok(()),
					Err(err) if is_retryable_transaction_conflict(&err) => {
						debug!(
							error = %err,
							"retryable conflict while cleaning uncommitted index build, retrying"
						);
						sleep(UNCOMMITTED_BUILD_CLEANUP_RETRY_SLEEP).await;
					}
					Err(err) => return Err(err),
				}
			}
		})
	}
}
