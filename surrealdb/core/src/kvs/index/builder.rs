use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Result, ensure};
use chrono::Utc;
use futures::channel::oneshot::{Receiver, Sender, channel};
#[cfg(not(target_family = "wasm"))]
use tokio::spawn;
use tokio::sync::RwLock;
use tokio::time::sleep;
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local as spawn;
use web_time::Instant;

use super::state::{
	build_owner_expired, delete_durable_build_queues, durable_index_error_reason,
	durable_report_count, is_condition_not_met, report_status_from_phase,
};
use super::{
	AcquiredBuild, BUILD_CLOSING_SLEEP, BuildGeneration, IndexBuildPhase, IndexBuildReportStatus,
	IndexBuildState, IndexBuilding,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, Index, IndexDefinition, IndexId, NamespaceId, TableId};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::IndexKeyBase;
use crate::idx::index::IndexOperation;
use crate::key::index::all as index_all;
use crate::key::record;
use crate::kvs::LockType::Optimistic;
use crate::kvs::ds::TransactionFactory;
#[cfg(test)]
use crate::kvs::testing::{RetryableConflictSite, maybe_inject_retryable_conflict};
use crate::kvs::{
	INDEXING_BATCH_SIZE, Transaction, TransactionType, is_retryable_transaction_conflict,
};
use crate::mem::ALLOC;
use crate::val::{RecordId, TableName, Value};

/// Process-local key used only to deduplicate active builder tasks.
pub(super) type SharedIndexKey = Arc<IndexKey>;

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
					Error::IndexAlreadyBuilding {
						name: building.ix.name.to_string(),
					}
				);
			}
			indexes.insert(Arc::clone(&building.ix_key), Arc::clone(&building));
		}
		let b = Arc::clone(&building);
		spawn(async move {
			let guard = BuildingFinishGuard(Arc::clone(&b));
			let r = b.run_acquired(acquired).await;
			let generation = b.build_generation.load(Ordering::Acquire);
			if let Err(err) = &r {
				let reason = err.to_string();
				if generation != 0 {
					let _ = b.mark_durable_error(generation, reason.clone()).await;
				}
			} else if b.aborted.load(Ordering::Acquire) && generation != 0 {
				let _ = b.mark_durable_aborted(generation).await;
			}
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
				return Err(Error::IndexingBuildingCancelled {
					reason: format!("Index {} build state no longer exists", building.ix.name),
				}
				.into());
			};
			match state.phase {
				IndexBuildPhase::Online => return Ok(()),
				IndexBuildPhase::Error => {
					return Err(Error::IndexingBuildingCancelled {
						reason: durable_index_error_reason(&building.ix, &state),
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
						return r.await.map_err(|_| Error::IndexingBuildingCancelled {
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
		ix.expect_not_prepare_remove()?;
		let (ns, db) = ctx.expect_ns_db_ids(&opt).await?;
		let key = Arc::new(IndexKey::new(ns, db, &ix.table_name, ix.index_id));
		let (rcv, sdr) = if blocking {
			let (s, r) = channel();
			(Some(r), Some(s))
		} else {
			(None, None)
		};
		if let Some(existing) = self.indexes.read().await.get(&key) {
			ensure!(
				existing.is_finished(),
				Error::IndexAlreadyBuilding {
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
			let existing = tx.get(&state_key, None).await?;
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
				let res = tx.putc(&state_key, &next, Some(current)).await;
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
			// New-generation takeover: drain any prior-generation `!br` from
			// in-flight writers (possibly on other cluster nodes) before
			// wiping durable build state. Otherwise the wipe destroys the
			// `!br` anchor of a writer mid-transaction and the new build's
			// initial scan can start before that writer's commit lands —
			// leaving its main-table writes invisible to the scan and its
			// `!bg(prior_gen, *)` orphaned by the wipe.
			tx.cancel().await?;
			self.wait_for_prior_generation_reservations().await?;

			// Re-read state in a fresh transaction; another node in the
			// cluster may have completed its own takeover while we were
			// draining. If state is now `Building`/`Closing`, restart so the
			// takeover-same-gen branch above handles it.
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let existing = tx.get(&state_key, None).await?;
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
			};
			delete_durable_build_queues(&tx, &self.ikb).await?;
			let res = tx.putc(&state_key, &state, existing.as_ref()).await;
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
					self.build_generation.store(generation, Ordering::Release);
					return Ok(Some(AcquiredBuild {
						generation,
						phase: IndexBuildPhase::Building,
						initial_complete: false,
						initial_count: 0,
						updates_count: 0,
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

	/// Read the durable build-state record without changing ownership.
	async fn read_durable_build_state(&self) -> Result<Option<IndexBuildState>> {
		let tx = self.new_read_tx().await?;
		let state = catch!(tx, tx.get(&self.ikb.new_bs_key(), None).await);
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
			let Some(current) = tx.get(&state_key, None).await? else {
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
			let res = tx.putc(&state_key, &next, Some(&current)).await;
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
		mut update: F,
	) -> Result<IndexBuildState>
	where
		F: FnMut(&mut IndexBuildState),
	{
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let state_key = self.ikb.new_bs_key();
			let Some(current) = tx.get(&state_key, None).await? else {
				tx.cancel().await?;
				return Err(Error::CorruptedIndex(
					"Index build state is missing during state update",
				)
				.into());
			};
			if current.generation != generation || current.owner != Some(self.owner) {
				tx.cancel().await?;
				return Err(Error::IndexingBuildingCancelled {
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
			let res = tx.putc(&state_key, &next, Some(&current)).await;
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
				state.error = None;
			}
		})
		.await?;
		Ok(())
	}

	/// Enter `Closing`, which blocks new admissions before the final drain.
	pub(super) async fn mark_durable_closing(&self, generation: BuildGeneration) -> Result<()> {
		self.update_owned_build_state(generation, |state| {
			if state.phase == IndexBuildPhase::Building {
				state.phase = IndexBuildPhase::Closing;
				state.error = None;
				state.report_status = Some(IndexBuildReportStatus::Indexing);
			}
		})
		.await?;
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
			let Some(current) = tx.get(&state_key, None).await? else {
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
			let res = tx.putc(&state_key, &next, Some(&current)).await;
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
		let Some(current) = tx.get(&state_key, None).await? else {
			return Err(Error::CorruptedIndex(
				"Index build state is missing during ownership heartbeat",
			)
			.into());
		};
		if current.generation != generation
			|| current.owner != Some(self.owner)
			|| !allowed.contains(&current.phase)
		{
			return Err(Error::IndexingBuildingCancelled {
				reason: format!("Index build ownership was lost for {}", self.ix.name),
			}
			.into());
		}
		let mut next = current.clone();
		let now = Utc::now();
		next.updated_at = now;
		next.owner_heartbeat_at = Some(now);
		tx.putc(&state_key, &next, Some(&current)).await?;
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

	async fn commit_and_retryable_conflict(&self, tx: &Transaction, action: &str) -> Result<bool> {
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
		self.tf
			.transaction(TransactionType::Read, Optimistic, self.ctx.try_get_sequences()?.clone())
			.await
	}

	pub(super) async fn new_write_tx_ctx(&self) -> Result<FrozenContext> {
		let tx = self
			.tf
			.transaction(TransactionType::Write, Optimistic, self.ctx.try_get_sequences()?.clone())
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
			.transaction(TransactionType::Read, Optimistic, self.ctx.try_get_sequences()?.clone())
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
			.get_tb_index(self.ix_key.ns, self.ix_key.db, &self.ix.table_name, &self.ix.name, None)
			.await?
		{
			ix.expect_not_prepare_remove()?;
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
		let Some(state) = tx.get(&self.ikb.new_bs_key(), None).await? else {
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
	pub(super) async fn run(&self) -> Result<()> {
		let Some(acquired) = self.acquire_build_state().await? else {
			return Ok(());
		};
		let res = self.run_acquired(acquired).await;
		if res.is_ok() && self.aborted.load(Ordering::Acquire) {
			let _ = self.mark_durable_aborted(acquired.generation).await;
		}
		res
	}

	/// Execute a build after durable ownership has already been acquired.
	///
	/// Takeover from `Building` with an incomplete initial scan restarts the
	/// scan after cleaning index data for this generation. Takeover from
	/// `Closing`, or from `Building` after `initial_complete`, skips the initial
	/// scan and only drains durable appendings and reservations.
	pub(super) async fn run_acquired(&self, acquired: AcquiredBuild) -> Result<()> {
		let mut last_prepare_remove_check = Instant::now();
		let generation = acquired.generation;
		let restarting_initial_scan =
			acquired.phase == IndexBuildPhase::Building && !acquired.initial_complete;
		// A restarted incomplete scan discards previous progress because it first
		// cleans index data. Resumed builds skip the scan, so keep the durable
		// counters that have already been reported for this generation.
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
			loop {
				if self.is_aborted().await {
					return Ok(());
				}
				let ctx = self.new_write_tx_ctx().await?;
				let key =
					index_all::new(self.ix_key.ns, self.ix_key.db, &self.ix_key.tb, self.ix_key.ix);
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
				if let Err(err) = tx.delp(&key).await {
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

			// First pass: index every record.
			let beg = record::prefix(self.ix_key.ns, self.ix_key.db, self.ikb.table())?;
			let end = record::suffix(self.ix_key.ns, self.ix_key.db, self.ikb.table())?;
			let mut next = Some(beg..end);
			let mut v1_appending_sentinel = false;
			let mut count_primary_cursor = matches!(self.ix.index, Index::Count(_)).then_some(None);
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
					let res = catch!(tx, tx.batch_keys_vals(rng, INDEXING_BATCH_SIZE, None).await);
					tx.cancel().await?;
					res
				};
				// Set the next scan range
				next = batch.next;
				// Check whether any records remain.
				if batch.result.is_empty() {
					// If not, initial indexing is complete.
					break;
				}
				// Create a new context with a write transaction.
				{
					let values = batch.result;
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
					match tx.commit().await {
						Ok(()) => break indexed,
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
			}
			// Mark initial build as complete before entering the appending phase.
			self.mark_durable_initial_complete(generation).await?;
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
		self.mark_durable_online(generation, initial_count, updates_count).await?;
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
				&self.ix,
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
				&self.ix,
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

	/// Check if the indexing process is aborting.
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
			Err(anyhow::Error::new(Error::QueryBeyondMemoryThreshold))
		} else {
			Ok(())
		}
	}

	pub(super) fn is_finished(&self) -> bool {
		self.finished.load(Ordering::Relaxed)
	}
}

struct BuildingFinishGuard(IndexBuilding);

impl Drop for BuildingFinishGuard {
	fn drop(&mut self) {
		self.0.finished.store(true, Ordering::Relaxed);
	}
}
