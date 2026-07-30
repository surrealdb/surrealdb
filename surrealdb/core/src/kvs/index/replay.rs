use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::Result;
use chrono::Utc;
use reblessive::TreeStack;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use uuid::Uuid;
use web_time::Instant;

use super::builder::Building;
use super::{
	AppendingId, BUILD_CLOSING_SLEEP, BatchId, BuildGeneration, BuildTicket,
	BuildTicketMutationSeq, ExistingPrimaryAppending, IndexBuildPhase, IndexBuildReportStatus,
	LEGACY_BATCH_ID,
};
use crate::catalog::providers::{NodeProvider, TableProvider};
use crate::catalog::{Error as CatalogError, Index, Record};
use crate::ctx::FrozenContext;
use crate::doc::{CursorDoc, Document};
use crate::exe::FlowResultExt as _;
use crate::idx::IndexKeyBase;
use crate::idx::docids::{DocId, TableDocIds};
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::index::IndexOperation;
use crate::key::index::ig::IndexAppending;
use crate::key::table::bg::Bg;
use crate::key::table::bp::Bp;
use crate::key::table::br::Br;
use crate::key::table::dp::{self, Dp};
use crate::key::{KVKeyDecode, KVRange, impl_kv_value_revisioned, record};
use crate::kvs::{
	DatastoreError, INDEXING_BATCH_SIZE, Transaction, Val, is_retryable_transaction_conflict,
};
use crate::val::{RecordId, RecordIdKey, Value};

/// Maximum consecutive commit-conflict retries for one deferred doc-ID reclaim
/// sweep chunk (and for its repair pass) before the sweep gives up.
///
/// The sweep runs after the build is durably `Online` and its `!dp` markers are
/// durable, so giving up loses nothing — leftover markers are drained by the
/// next doc-ID index build on the table. The bound exists so a conflict storm
/// cannot wedge the finished build task (and, for a blocking `DEFINE INDEX`,
/// the statement waiting on it).
const DOC_ID_RECLAIM_MAX_RETRIES: usize = 10;

#[revisioned(revision = 2)]
#[derive(Debug, PartialEq)]
pub(crate) struct Appending {
	/// Values to remove from the index when replaying the write.
	pub(super) old_values: Option<Vec<Value>>,
	/// Values to add to the index when replaying the write.
	pub(super) new_values: Option<Vec<Value>>,
	/// Record id key whose index entries are being replayed.
	pub(super) id: RecordIdKey,
	/// Cached COUNT condition match state `(old_matches, new_matches)`.
	///
	/// Re-evaluating a conditional COUNT predicate during replay can observe a
	/// different document state than the user write observed. Carrying both
	/// booleans makes replay deterministic.
	#[revision(start = 2)]
	pub(super) count_cond_match: Option<(bool, bool)>,
}

impl_kv_value_revisioned!(Appending);

impl Appending {
	#[cfg(test)]
	pub(crate) fn new(
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
		id: RecordIdKey,
	) -> Self {
		Self {
			old_values,
			new_values,
			id,
			count_cond_match: None,
		}
	}
}

#[revisioned(revision = 2)]
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub(crate) struct PrimaryAppending(
	/// Appending id within the concurrent indexing queue.
	AppendingId,
	/// Batch id associated with this append.
	#[revision(start = 2)]
	BatchId,
);

impl_kv_value_revisioned!(PrimaryAppending);

/// Pointer from a per-record `!bp` marker to the specific `!bg` entry that
/// holds the writer-observed old state for that record.
///
/// One reservation is allocated per user transaction per index, so the same
/// `ticket` can cover many `!bg` entries. The `mutation_seq` selects the
/// first admitted mutation for the marker's record.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PrimaryAppendingTicket {
	pub(crate) ticket: BuildTicket,
	pub(crate) mutation_seq: BuildTicketMutationSeq,
}

impl_kv_value_revisioned!(PrimaryAppendingTicket);

impl PrimaryAppending {
	#[cfg(test)]
	pub(crate) fn new(appending_id: AppendingId, batch_id: BatchId) -> Self {
		Self(appending_id, batch_id)
	}
}

struct InitialIndexValue<'a> {
	rid: &'a RecordId,
	opt_values: Option<Vec<Value>>,
	count_cond_match: Option<(bool, bool)>,
}

struct CountPrimaryAppendingScan<'a> {
	lookup_tx: &'a Transaction,
	cursor: &'a mut Option<RecordIdKey>,
	through: Option<&'a RecordIdKey>,
	live_ids: &'a BTreeSet<RecordIdKey>,
	initial_count: usize,
}

impl Building {
	/// Drain both legacy and durable queued mutations until the visible queues are empty.
	///
	/// Legacy `!ig`/`!ip` appendings can exist from already committed work
	/// created by older paths. Durable `!bg` appendings are scoped to the
	/// current generation and must be replayed before the build can move online.
	pub(super) async fn index_appending_loop(
		&self,
		initial_count: usize,
		updates_count: &mut usize,
		last_prepare_remove_check: &mut Instant,
	) -> Result<()> {
		let rng = self.ikb.new_ig_range()?;
		let generation = self.build_generation.load(Ordering::Acquire);
		let durable_rng = if generation == 0 {
			None
		} else {
			Some(self.ikb.new_bg_range(generation)?)
		};
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(None)?;
			// Check the index still exists and has not been marked for removal
			self.check_prepare_remove(last_prepare_remove_check).await?;

			let (keys, durable_keys) = {
				let tx = self.new_read_tx().await?;
				let keys = catch!(tx, tx.keys(rng.clone(), INDEXING_BATCH_SIZE, 0, None).await);
				let durable_keys = if let Some(durable_rng) = &durable_rng {
					catch!(tx, tx.keys(durable_rng.clone(), INDEXING_BATCH_SIZE, 0, None).await)
				} else {
					Vec::new()
				};
				tx.cancel().await?;
				(keys, durable_keys)
			};
			let pending = keys.len() + durable_keys.len();
			if keys.is_empty() && durable_keys.is_empty() {
				self.mark_durable_report(
					generation,
					IndexBuildReportStatus::Indexing,
					Some(initial_count),
					Some(0),
					Some(*updates_count),
				)
				.await?;
				break;
			}
			self.mark_durable_report(
				generation,
				IndexBuildReportStatus::Indexing,
				Some(initial_count),
				Some(pending),
				Some(*updates_count),
			)
			.await?;
			if !keys.is_empty() {
				// We have committed appendings to index.
				// Create a new context with a write transaction.
				let ctx = self.new_write_tx_ctx().await?;
				let tx = ctx.tx();
				let saved_updates_count = *updates_count;
				let allowed = if generation == 0 {
					&[][..]
				} else {
					&[IndexBuildPhase::Building, IndexBuildPhase::Closing][..]
				};
				if generation != 0
					&& let Err(err) = self.maintain_build_ownership(&tx, generation, allowed).await
				{
					*updates_count = saved_updates_count;
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
				match self.index_appending_range(&ctx, &tx, keys, updates_count).await {
					Ok(()) => {}
					Err(err) => {
						*updates_count = saved_updates_count;
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict in appending range, retrying",
							)
							.await
						{
							continue;
						}
						return Err(err);
					}
				};
				match tx.commit().await {
					Ok(()) => {
						self.mark_durable_report(
							generation,
							IndexBuildReportStatus::Indexing,
							Some(initial_count),
							Some(0),
							Some(*updates_count),
						)
						.await?;
					}
					Err(err) => {
						*updates_count = saved_updates_count;
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict on commit, retrying",
							)
							.await
						{
							continue;
						}
						return Err(err);
					}
				}
			}
			if !durable_keys.is_empty() {
				let ctx = self.new_write_tx_ctx().await?;
				let tx = ctx.tx();
				let saved_updates_count = *updates_count;
				let allowed = &[IndexBuildPhase::Building, IndexBuildPhase::Closing];
				if let Err(err) = self.maintain_build_ownership(&tx, generation, allowed).await {
					*updates_count = saved_updates_count;
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
				match self
					.index_durable_appending_range(&ctx, &tx, durable_keys, updates_count)
					.await
				{
					Ok(()) => match tx.commit().await {
						Ok(()) => {
							self.mark_durable_report(
								generation,
								IndexBuildReportStatus::Indexing,
								Some(initial_count),
								Some(0),
								Some(*updates_count),
							)
							.await?;
						}
						Err(err) => {
							*updates_count = saved_updates_count;
							if self
								.cancel_and_retryable_conflict(
									&tx,
									&err,
									"transient conflict on durable appending commit, retrying",
								)
								.await
							{
								continue;
							}
							return Err(err);
						}
					},
					Err(err) => {
						*updates_count = saved_updates_count;
						if self
							.cancel_and_retryable_conflict(
								&tx,
								&err,
								"transient conflict in durable appending range, retrying",
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
		Ok(())
	}

	/// Wait until every admitted writer has either committed its appending or is
	/// safe to discard after writer-node death.
	///
	/// During `Closing`, new admissions are rejected. This loop closes the gap
	/// for writers that received a ticket before `Closing` but have not yet
	/// committed the user transaction that writes the durable appending.
	pub(super) async fn wait_for_durable_reservations(
		&self,
		generation: BuildGeneration,
		last_prepare_remove_check: &mut Instant,
	) -> Result<()> {
		let rng = self.ikb.new_br_range(generation)?;
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			self.check_prepare_remove(last_prepare_remove_check).await?;
			let keys = {
				let tx = self.new_read_tx().await?;
				let keys = catch!(tx, tx.keys(rng.clone(), INDEXING_BATCH_SIZE, 0, None).await);
				tx.cancel().await?;
				keys
			};
			if keys.is_empty() {
				return Ok(());
			}
			let now = Utc::now();
			let mut blocked = false;
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			if let Err(err) =
				self.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Closing]).await
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
			for key in keys {
				let br = Br::decode_key(&key)?;
				if let Some(reservation) = tx.get_key(&br, None).await? {
					// One reservation now covers an entire user transaction's
					// batch of mutations on this index, so existence of any
					// `!bg(generation, ticket, *)` entry signals that the
					// writer committed at least one mutation under this
					// ticket. Either case (any !bg present, or writer dead)
					// makes it safe to retire the reservation.
					let appending_committed = !tx
						.keys(self.ikb.new_bg_ticket_range(br.generation, br.ticket)?, 1, 0, None)
						.await?
						.is_empty();
					let writer_dead = reservation.expires_at <= now
						&& !self.reservation_node_is_live(&tx, reservation.node).await?;
					if appending_committed || writer_dead {
						tx.del_key(&br).await?;
					} else {
						blocked = true;
					}
				}
			}
			if let Err(err) = tx.commit().await {
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict while cleaning build reservations, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
			if blocked {
				sleep(BUILD_CLOSING_SLEEP).await;
			}
		}
	}

	/// Drain `!br` reservations of every generation strictly below `below`,
	/// after a fresh-generation takeover has installed the new state and
	/// before it wipes the stale queues.
	///
	/// A new-generation takeover (see `acquire_build_state` in `builder.rs`)
	/// deletes the old generations' `!br/!bg/!bp` entries. If a writer on
	/// **any** node in the cluster is mid-transaction with a cached
	/// `!br(prior_gen, ticket)`, that wipe destroys the anchor the protocol
	/// relies on to keep the writer's commit visible to the new build's
	/// initial scan. The takeover therefore installs the new generation
	/// FIRST, in the same transaction that removes the previous generation's
	/// `!bt` ticket counter — allocation compare-and-swaps that counter, and
	/// the admission fence rejects generation mismatches, so no further
	/// old-generation reservations can be created (builds in `Error` admit
	/// like `Building`) — and only then drains, which makes the empty state
	/// this loop waits for stable.
	///
	/// This loop blocks the takeover until every below-`below` `!br` is
	/// either gone (the writer's user transaction committed, the deferred
	/// release ran, or another drainer in the cluster removed it) or the
	/// writer's node is confirmed dead via durable membership. Once the scan
	/// returns empty, the new build's initial scan is guaranteed to start
	/// after every surviving writer's commit — so the writer's main-table
	/// writes are visible to the scan even though the `!bg(prior_gen, *)`
	/// entries are about to be wiped. A live writer holding a transaction
	/// open can block this (and the takeover) until it closes; the new
	/// generation's owner heartbeat is not refreshed while waiting, so a
	/// long-blocked takeover can itself be taken over after lease expiry —
	/// ownership CAS fencing keeps that safe.
	///
	/// Classification matches `wait_for_durable_reservations` and uses only
	/// durable state, so it works cross-node without in-process coordination.
	/// Concurrent drainers race safely: each `tx.del(&br)` is a no-op once
	/// another node has already removed the entry.
	pub(super) async fn wait_for_prior_generation_reservations(
		&self,
		below: BuildGeneration,
	) -> Result<()> {
		// Reservations sort by generation, so the stale span is everything
		// before the start of `below`'s own range.
		let mut rng = self.ikb.new_br_all_generations_range()?;
		rng.end = self.ikb.new_br_range(below)?.start;
		loop {
			if self.is_aborted().await {
				return Ok(());
			}
			let keys = {
				let tx = self.new_read_tx().await?;
				let keys = catch!(tx, tx.keys(rng.clone(), INDEXING_BATCH_SIZE, 0, None).await);
				tx.cancel().await?;
				keys
			};
			if keys.is_empty() {
				return Ok(());
			}
			let now = Utc::now();
			let mut blocked = false;
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			for key in keys {
				let br = Br::decode_key(&key)?;
				if let Some(reservation) = tx.get_key(&br, None).await? {
					// Same retire test as `wait_for_durable_reservations`: any
					// committed `!bg(gen, ticket, *)` means the writer's user
					// transaction committed (its main-table writes are durable),
					// and a TTL-expired reservation owned by an inactive node
					// means no further commit will arrive.
					let appending_committed = !tx
						.keys(self.ikb.new_bg_ticket_range(br.generation, br.ticket)?, 1, 0, None)
						.await?
						.is_empty();
					let writer_dead = reservation.expires_at <= now
						&& !self.reservation_node_is_live(&tx, reservation.node).await?;
					if appending_committed || writer_dead {
						tx.del_key(&br).await?;
					} else {
						blocked = true;
					}
				}
			}
			if let Err(err) = tx.commit().await {
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict draining prior-generation reservations, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
			if blocked {
				sleep(BUILD_CLOSING_SLEEP).await;
			}
		}
	}

	/// Check durable node membership before discarding an expired reservation.
	async fn reservation_node_is_live(&self, tx: &Transaction, node: Uuid) -> Result<bool> {
		match tx.get_node(node).await {
			Ok(node) => Ok(node.is_active()),
			Err(err)
				if matches!(
					err.downcast_ref::<CatalogError>(),
					Some(CatalogError::NdNotFound { .. })
				) =>
			{
				Ok(false)
			}
			Err(err) => Err(err),
		}
	}

	/// Index one batch from the initial record scan.
	///
	/// If a queued mutation already exists for a record, the scan indexes the
	/// queued old state instead of the current record value. That avoids
	/// double-counting the same write when the appending is replayed later.
	pub(super) async fn index_initial_batch(
		&self,
		ctx: &FrozenContext,
		tx: &Transaction,
		values: &[(Vec<u8>, Val)],
		initial_count: usize,
		v1_appending_sentinel: &mut bool,
		count_primary_cursor: &mut Option<Option<RecordIdKey>>,
	) -> Result<usize> {
		let mut rc = false;
		let mut count = 0;
		let mut live_ids = BTreeSet::new();
		let mut last_live_id = None;
		let mut stack = TreeStack::new();
		let fulltext_index =
			IndexOperation::create_fulltext_index(ctx, self.ix_key.ns, self.ix_key.db, &self.ix)
				.await?;
		let lookup_tx = self.new_read_tx().await?;
		// COUNT WHERE predicate, already parsed once when the build started.
		let count_cond_expr: Option<crate::expr::Expr> =
			self.ix.count_cond.as_ref().map(|c| c.0.clone());
		let result = async {
			// Index the records.
			for (k, v) in values {
				// Per-record analysis (deserialization, tokenization, filters)
				// is synchronous CPU work with no other guaranteed await; yield
				// so a long batch cannot starve the runtime worker.
				yield_now!();
				if self.is_aborted().await {
					return Ok(count);
				}
				self.is_beyond_threshold(Some(initial_count + count))?;
				let key = record::RecordKey::decode_key(k)?;
				// Parse the value.
				let val: Record = revision::from_slice(v.as_slice())?;
				let rid: Arc<RecordId> = RecordId {
					table: key.tb.into_owned(),
					key: key.id.into_owned(),
				}
				.into();
				if count_primary_cursor.is_some() {
					live_ids.insert(rid.key.clone());
					last_live_id = Some(rid.key.clone());
				}

				// Is there already a queued update for this record?
				let (opt_values, count_cond_match) = if let Some(a) = self
					.check_existing_primary_appending(&lookup_tx, &rid.key, v1_appending_sentinel)
					.await?
				{
					(a.old_values, a.count_cond_match.map(|(old_matches, _)| (false, old_matches)))
				} else {
					// Otherwise, proceed with normal indexing.
					let doc = CursorDoc::new(Some(Arc::clone(&rid)), None, val);
					let opt_values = stack
						.enter(|stk| {
							Document::build_opt_values(stk, ctx, &self.opt, &self.ix, &doc)
						})
						.finish()
						.await?;
					// COUNT WHERE indexes have no indexed values, so the
					// initial scan carries the predicate result separately.
					let count_cond_match = if let Some(expr) = &count_cond_expr {
						let new_matches = stack
							.enter(|stk| {
								crate::legacy::expr_compute(expr, stk, ctx, &self.opt, Some(&doc))
							})
							.finish()
							.await
							.catch_return()?
							.is_truthy();
						Some((false, new_matches))
					} else {
						None
					};
					(opt_values, count_cond_match)
				};
				self.index_initial_values(
					ctx,
					&mut stack,
					&fulltext_index,
					InitialIndexValue {
						rid: rid.as_ref(),
						opt_values,
						count_cond_match,
					},
					&mut rc,
				)
				.await?;

				// Increment the count.
				count += 1;
			}
			if let Some(cursor) = count_primary_cursor.as_mut()
				&& let Some(through) = last_live_id.as_ref()
			{
				count += self
					.index_missing_count_primary_appendings(
						ctx,
						CountPrimaryAppendingScan {
							lookup_tx: &lookup_tx,
							cursor,
							through: Some(through),
							live_ids: &live_ids,
							initial_count: initial_count + count,
						},
						&mut stack,
						&mut rc,
					)
					.await?;
			}
			// Trigger compaction if needed.
			self.check_index_compaction(tx, &mut rc).await?;
			// We're done.
			Ok(count)
		}
		.await;
		let cancel_result = lookup_tx.cancel().await;
		match result {
			Ok(count) => {
				cancel_result?;
				Ok(count)
			}
			Err(err) => {
				let _ = cancel_result;
				Err(err)
			}
		}
	}

	/// Index queued old states for COUNT records the live scan has already passed.
	pub(super) async fn index_remaining_count_primary_appendings(
		&self,
		ctx: &FrozenContext,
		tx: &Transaction,
		count_primary_cursor: &mut Option<Option<RecordIdKey>>,
		initial_count: usize,
	) -> Result<usize> {
		let Some(cursor) = count_primary_cursor.as_mut() else {
			return Ok(0);
		};
		let lookup_tx = self.new_read_tx().await?;
		let mut stack = TreeStack::new();
		let mut rc = false;
		let live_ids = BTreeSet::new();
		let result = self
			.index_missing_count_primary_appendings(
				ctx,
				CountPrimaryAppendingScan {
					lookup_tx: &lookup_tx,
					cursor,
					through: None,
					live_ids: &live_ids,
					initial_count,
				},
				&mut stack,
				&mut rc,
			)
			.await;
		let cancel_result = lookup_tx.cancel().await;
		match result {
			Ok(count) => {
				cancel_result?;
				self.check_index_compaction(tx, &mut rc).await?;
				Ok(count)
			}
			Err(err) => {
				let _ = cancel_result;
				Err(err)
			}
		}
	}

	/// Index one initial-scan value.
	async fn index_initial_values(
		&self,
		ctx: &FrozenContext,
		stack: &mut TreeStack,
		fulltext_index: &Option<FullTextIndex>,
		value: InitialIndexValue<'_>,
		rc: &mut bool,
	) -> Result<()> {
		let InitialIndexValue {
			rid,
			opt_values,
			count_cond_match,
		} = value;
		let mut io = IndexOperation::new(
			ctx,
			&self.opt,
			self.ix_key.ns,
			self.ix_key.db,
			self.tb,
			&self.ix,
			None,
			opt_values,
			rid,
		);
		if let Some((old_matches, new_matches)) = count_cond_match {
			io = io.with_count_cond_match(old_matches, new_matches);
		}
		if let Some(fulltext_index) = fulltext_index {
			stack
				.enter(|stk| io.compute_fulltext_with_index(stk, fulltext_index, rc))
				.finish()
				.await
		} else {
			stack.enter(|stk| io.compute(stk, rc)).finish().await
		}
	}

	/// Merge orphaned COUNT primary markers into the initial baseline.
	///
	/// Durable `!bp` markers record the first queued mutation for each record
	/// while the initial scan is running. A COUNT build must index the queued
	/// old state even when the live record scan no longer sees the record, so
	/// later replay deltas are applied against the same baseline.
	async fn index_missing_count_primary_appendings(
		&self,
		ctx: &FrozenContext,
		scan: CountPrimaryAppendingScan<'_>,
		stack: &mut TreeStack,
		rc: &mut bool,
	) -> Result<usize> {
		let Index::Count(_) = &self.ix.index else {
			return Ok(0);
		};
		let generation = self.build_generation.load(Ordering::Acquire);
		if generation == 0 {
			return Ok(0);
		}
		let range = self.ikb.new_bp_span_range(generation, scan.cursor.as_ref(), scan.through)?;
		if range.start >= range.end {
			if let Some(through) = scan.through {
				*scan.cursor = Some(through.clone());
			}
			return Ok(0);
		}
		let mut count = 0;
		let mut next = Some(range);
		while let Some(rng) = next {
			if self.is_aborted().await {
				return Ok(count);
			}
			let batch = scan.lookup_tx.batch_keys(rng, INDEXING_BATCH_SIZE, None).await?;
			next = batch.next;
			for key in batch.result {
				// Baseline indexing is synchronous CPU work; yield so a long
				// batch cannot starve the runtime worker.
				yield_now!();
				if self.is_aborted().await {
					return Ok(count);
				}
				self.is_beyond_threshold(Some(scan.initial_count + count))?;
				let bp = Bp::decode_key(&key)?;
				if scan.live_ids.contains(&bp.id) {
					continue;
				}
				let Some(ptr) = scan.lookup_tx.get_key(&bp, None).await? else {
					continue;
				};
				let bg = self.ikb.new_bg_key(bp.generation, ptr.ticket, ptr.mutation_seq);
				let Some(appending) = scan.lookup_tx.get_key(&bg, None).await? else {
					return Err(DatastoreError::CorruptedIndex(
						"Durable appending record is missing",
					)
					.into());
				};
				let rid = RecordId {
					table: self.ikb.table().clone(),
					key: bp.id.into_owned(),
				};
				let count_cond_match =
					appending.count_cond_match.map(|(old_matches, _)| (false, old_matches));
				self.index_initial_values(
					ctx,
					stack,
					&None,
					InitialIndexValue {
						rid: &rid,
						opt_values: appending.old_values,
						count_cond_match,
					},
					rc,
				)
				.await?;
				count += 1;
			}
		}
		if let Some(through) = scan.through {
			*scan.cursor = Some(through.clone());
		}
		Ok(count)
	}

	/// Look up an existing per-record appending marker before initial indexing.
	async fn check_existing_primary_appending(
		&self,
		lookup_tx: &Transaction,
		id_key: &RecordIdKey,
		v1_appending_sentinel: &mut bool,
	) -> Result<Option<Appending>> {
		match self.load_existing_primary_appending(lookup_tx, id_key).await? {
			ExistingPrimaryAppending::None => Ok(None),
			ExistingPrimaryAppending::Appending(appending) => Ok(Some(appending)),
			ExistingPrimaryAppending::Legacy => {
				self.cleanup_legacy_primary_appending(id_key).await?;
				if !*v1_appending_sentinel {
					*v1_appending_sentinel = true;
					warn!(
						"Found legacy v1 primary appending entry from an older version; legacy queued updates will be ignored. Consider rebuilding index {} on table {}.",
						self.ix.name, self.ix.table_name
					);
				}
				Ok(None)
			}
		}
	}

	/// Load the queued mutation that should replace the current record value.
	///
	/// Durable `!bp` markers point to generation-scoped `!bg` entries. Legacy
	/// `!ip` markers point to `!ig` entries unless they are old v1 markers with
	/// no batch id, which cannot be resolved safely and are cleaned up.
	async fn load_existing_primary_appending(
		&self,
		tx: &Transaction,
		id_key: &RecordIdKey,
	) -> Result<ExistingPrimaryAppending> {
		let generation = self.build_generation.load(Ordering::Acquire);
		if generation != 0 {
			let bp = self.ikb.new_bp_key(generation, id_key);
			if let Some(ptr) = tx.get_key(&bp, None).await? {
				let bg = self.ikb.new_bg_key(generation, ptr.ticket, ptr.mutation_seq);
				let Some(appending) = tx.get_key(&bg, None).await? else {
					return Err(DatastoreError::CorruptedIndex(
						"Durable appending record is missing",
					)
					.into());
				};
				return Ok(ExistingPrimaryAppending::Appending(appending));
			}
		}
		let ip = self.ikb.new_ip_key(id_key.clone());
		let Some(pa) = tx.get_key(&ip, None).await? else {
			return Ok(ExistingPrimaryAppending::None);
		};
		// Use the old values from the queued update as the initial indexing input.
		if pa.1 == LEGACY_BATCH_ID {
			return Ok(ExistingPrimaryAppending::Legacy);
		}
		let ig = self.ikb.new_ig_key(pa.0, pa.1);
		let Some(appending) = tx.get_key(&ig, None).await? else {
			return Err(DatastoreError::CorruptedIndex("Appending record is missing").into());
		};
		Ok(ExistingPrimaryAppending::Appending(appending))
	}

	async fn cleanup_legacy_primary_appending(&self, id_key: &RecordIdKey) -> Result<()> {
		// Legacy v1 primary appending entries have no batch id and cannot be resolved to a
		// current !ig record. Clean them outside the initial-build write transaction so
		// normal concurrent appends cannot make that transaction conflict.
		let ctx = self.new_write_tx_ctx().await?;
		let tx = ctx.tx();
		let ip = self.ikb.new_ip_key(id_key.clone());
		let pa = catch!(tx, tx.get_key(&ip, None).await);
		if matches!(pa, Some(pa) if pa.1 == LEGACY_BATCH_ID) {
			catch!(tx, tx.del_key(&ip).await);
		}
		let res = tx.commit().await;
		match res {
			Ok(()) => Ok(()),
			Err(err) if is_retryable_transaction_conflict(&err) => {
				let _ = tx.cancel().await;
				warn!(
					"{}: transient conflict while cleaning legacy primary appending entry; continuing",
					self.ix.name
				);
				Ok(())
			}
			Err(err) => {
				let _ = tx.cancel().await;
				Err(err)
			}
		}
	}

	/// Apply one queued mutation and return the record key for queue cleanup.
	async fn apply_appending(
		&self,
		ctx: &FrozenContext,
		stack: &mut TreeStack,
		fulltext_index: &Option<FullTextIndex>,
		appending: Appending,
		rc: &mut bool,
	) -> Result<RecordIdKey> {
		let rid_key = appending.id;
		// A queued delete (old present, new absent) on a doc-ID index is a consumer
		// of the shared doc-ID space: the delete path deferred the `!di`/`!dd`
		// removal precisely so this replay could still resolve the doc-ID (full-text
		// reads it; HNSW/DiskAnn capture it into the pending). The mapping is
		// table-scoped, so it may only be reclaimed by the LAST building consumer —
		// reclaiming it while a sibling doc-ID index is still building would leave
		// that sibling unable to resolve the doc-ID (guarded by
		// `other_doc_id_index_building` below).
		let reclaim_doc_id = appending.new_values.is_none()
			&& appending.old_values.is_some()
			&& self.ix.uses_doc_ids();
		let rid = RecordId {
			table: self.ikb.table().clone(),
			key: rid_key.clone(),
		};
		let mut io = IndexOperation::new(
			ctx,
			&self.opt,
			self.ix_key.ns,
			self.ix_key.db,
			self.tb,
			&self.ix,
			appending.old_values,
			appending.new_values,
			&rid,
		);
		if let Some((old_matches, new_matches)) = appending.count_cond_match {
			io = io.with_count_cond_match(old_matches, new_matches);
		}
		if let Some(fulltext_index) = fulltext_index {
			stack
				.enter(|stk| io.compute_fulltext_with_index(stk, fulltext_index, rc))
				.finish()
				.await?;
		} else {
			stack.enter(|stk| io.compute(stk, rc)).finish().await?;
		}
		if reclaim_doc_id {
			let tx = ctx.tx();
			if self.other_doc_id_index_building(ctx).await? {
				// A sibling doc-ID index is still building and may still need the
				// shared `!di`/`!dd` mapping to replay its own copy of this delete,
				// so the mapping must stay live. The delete transaction already
				// wrote a durable `!dp` marker when it deferred the removal (see
				// `doc::index`'s `defer_doc_id_removal`); re-assert it here —
				// atomically with the queue entry being consumed — so the pending
				// reclaim also holds for legacy `!ig` appendings, and is completed
				// by whichever doc-ID index build finishes once no sibling is
				// building (see
				// [`reclaim_deferred_doc_ids`](Self::reclaim_deferred_doc_ids)).
				self.defer_doc_id_reclaim(&tx, &rid_key).await?;
			} else {
				// Last (or only) building consumer: reclaim the shared mapping now
				// and consume the delete transaction's pending-reclaim marker.
				TableDocIds::new(self.ix_key.ns, self.ix_key.db, self.ikb.table().clone())
					.remove(&tx, &rid_key)
					.await?;
				let dp = Dp::new(self.ix_key.ns, self.ix_key.db, self.ikb.table(), &rid_key);
				tx.del_key(&dp).await?;
			}
		}
		Ok(rid_key)
	}

	/// Whether another doc-ID-consuming index on the same table is still building
	/// (durable build phase `Building`/`Closing`), and so may still need the shared
	/// `!di`/`!dd` mapping to replay its own copy of a queued delete.
	///
	/// Gates the reclaim in [`apply_appending`](Self::apply_appending): while a
	/// sibling is still building the mapping must be kept alive (each build resolves
	/// and removes its own entries against it), so the reclaim is deferred through a
	/// durable `!dp` marker and completed by
	/// [`reclaim_deferred_doc_ids`](Self::reclaim_deferred_doc_ids). The common case
	/// (a single building doc-ID index) finds no sibling and reclaims immediately;
	/// an already-`Online` sibling keeps its `!bs` state but is not counted, since
	/// it processed the delete synchronously.
	///
	/// The check counts a sibling by durable phase alone, deliberately ignoring
	/// owner-lease expiry: a stranded `Building` sibling is resumable, and its
	/// resumed build still needs the mapping to replay its unconsumed deletes, so
	/// keeping the mapping (and the durable marker) is the safe answer either way.
	async fn other_doc_id_index_building(&self, ctx: &FrozenContext) -> Result<bool> {
		let tx = ctx.tx();
		let indexes =
			tx.all_tb_indexes(self.ix_key.ns, self.ix_key.db, self.ikb.table(), None).await?;
		for other in indexes.iter() {
			if other.index_id == self.ix.index_id || !other.uses_doc_ids() {
				continue;
			}
			let other_ikb = IndexKeyBase::new(
				self.ix_key.ns,
				self.ix_key.db,
				self.ikb.table().clone(),
				other.index_id,
			);
			if let Some(state) = tx.get_key(&other_ikb.new_bs_key(), None).await?
				&& matches!(state.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				return Ok(true);
			}
		}
		Ok(false)
	}

	/// Records durably — in the same transaction that consumes the delete from
	/// this builder's queue — that the shared doc-ID mapping for `id` could not
	/// be reclaimed yet, because a sibling doc-ID index was still building and
	/// may still need it to replay its own copy of the delete.
	///
	/// The `!dp` marker is table-scoped, so it is visible to *every* doc-ID
	/// index build on the table — including builds that started after the delete
	/// and resumed/taken-over builds — and survives builder errors, aborts and
	/// crashes. It is consumed by
	/// [`reclaim_deferred_doc_ids`](Self::reclaim_deferred_doc_ids).
	async fn defer_doc_id_reclaim(&self, tx: &Transaction, id: &RecordIdKey) -> Result<()> {
		let dp = Dp::new(self.ix_key.ns, self.ix_key.db, self.ikb.table(), id);
		tx.set_key(&dp, &()).await
	}

	/// Drains the table's durable `!dp` pending-reclaim markers, reclaiming the
	/// shared `!di`/`!dd` mapping of every marked record that is still absent
	/// (see [`apply_appending`](Self::apply_appending)).
	///
	/// Runs best-effort, immediately after this builder publishes `Online`, and
	/// only when no sibling doc-ID index is still `Building`/`Closing` — the
	/// later-to-finish builder re-checks after its own `Online` commit, so it
	/// deterministically observes every earlier finisher as `Online`. If a
	/// sibling is still building, the sweep leaves the durable markers in place:
	/// they are drained by whichever doc-ID index build finishes next with no
	/// building sibling — including a build that started after the deletes, a
	/// resumed/taken-over build, or a later `REBUILD` — and
	/// [`TableDocIds::remove_all`] clears them when the table's last doc-ID
	/// index is dropped. A marker is therefore never lost, only completed later.
	///
	/// The sweep processes markers in [`INDEXING_BATCH_SIZE`] chunks, each in its
	/// own transaction, probing the marked records with one batched read:
	/// - a record that is still absent has its `!di`/`!dd` mapping reclaimed;
	/// - a record re-created during the build keeps its live mapping;
	/// - the `!dp` marker is consumed in both cases.
	///
	/// A record re-created *concurrently with a sweep chunk* can adopt the old
	/// id from its snapshot (a pure read, which no backend validates at commit)
	/// while the chunk reclaims that same mapping. Each chunk therefore re-probes
	/// the records it reclaimed after committing and restores the mapping of any
	/// that turn out live (see [`TableDocIds::restore`]); the doubly-raced
	/// residual (a re-create overlapping both the chunk and its repair probe)
	/// self-heals on the record's next indexed write. Duplicate sweeps (two
	/// builders finishing in lock-step) are harmless: reclaim and restore are
	/// both idempotent put-if-absent/delete shapes.
	pub(super) async fn reclaim_deferred_doc_ids(&self) -> Result<()> {
		// Only doc-ID-consuming index builds sweep; other index kinds never
		// defer and skip the range probe entirely.
		if !self.ix.uses_doc_ids() {
			return Ok(());
		}
		let docs = TableDocIds::new(self.ix_key.ns, self.ix_key.db, self.ikb.table().clone());
		let mut retries = 0usize;
		loop {
			// The sweep runs after `Online`, so a `REMOVE INDEX` abort is the
			// only cancellation signal left to honour.
			if self.is_aborted().await {
				return Ok(());
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			// A still-building sibling may still need the mappings to replay its
			// queued deletes: leave the durable markers for a later finisher.
			if catch!(tx, self.other_doc_id_index_building(&ctx).await) {
				tx.cancel().await?;
				return Ok(());
			}
			// Fetch the next chunk of pending markers.
			let rng = catch!(
				tx,
				dp::Prefix::new(self.ix_key.ns, self.ix_key.db, self.ikb.table()).encode_range()
			);
			let keys = catch!(tx, tx.keys(rng, INDEXING_BATCH_SIZE, 0, None).await);
			if keys.is_empty() {
				tx.cancel().await?;
				return Ok(());
			}
			let mut ids = Vec::with_capacity(keys.len());
			for k in &keys {
				ids.push(catch!(tx, Dp::decode_key(k)).into_id());
			}
			// One batched probe decides every record's fate in this chunk.
			let record_keys: Vec<record::RecordKey> =
				ids.iter().map(|id| self.record_key(id)).collect();
			let records = catch!(tx, tx.get_many_key(record_keys, None).await);
			// Records reclaimed in this chunk, remembered for the post-commit
			// re-probe that repairs concurrent re-creates.
			let mut reclaimed: Vec<(RecordIdKey, DocId)> = Vec::new();
			for (id, existing) in ids.iter().zip(records) {
				if existing.is_none()
					&& let Some(doc_id) = catch!(tx, docs.remove(&tx, id).await)
				{
					reclaimed.push((id.clone(), doc_id));
				}
				// The marker is consumed whether the mapping was reclaimed
				// (record absent), kept (record re-created during the build), or
				// already gone (reclaimed by an earlier inline pass or sweep).
				let dp = Dp::new(self.ix_key.ns, self.ix_key.db, self.ikb.table(), id);
				catch!(tx, tx.del_key(&dp).await);
			}
			if self
				.commit_and_retryable_conflict(
					&tx,
					"transient conflict reclaiming deferred doc-ID mappings, retrying",
				)
				.await?
			{
				// Retry the same chunk: the markers are still durable. Bounded so
				// a conflict storm cannot wedge the (already Online) build task —
				// leftover markers are picked up by the next doc-ID index build.
				retries += 1;
				if retries > DOC_ID_RECLAIM_MAX_RETRIES {
					warn!(
						index = %self.ix.name,
						table = %self.ix.table_name,
						"giving up the deferred doc-ID reclaim sweep after repeated \
						 conflicts; leftover markers will be reclaimed by the next \
						 doc-ID index build on the table"
					);
					return Ok(());
				}
				continue;
			}
			retries = 0;
			// Repair the mappings of records re-created while the chunk committed.
			self.repair_recreated_doc_ids(&docs, &reclaimed).await?;
		}
	}

	/// Builds the datastore key of one record on the indexed table.
	fn record_key<'a>(&'a self, id: &'a RecordIdKey) -> record::RecordKey<'a> {
		record::RecordKey {
			root: crate::key::database::all::DatabaseRoot {
				ns: self.ix_key.ns,
				db: self.ix_key.db,
			},
			tb: std::borrow::Cow::Borrowed(self.ikb.table()),
			id: std::borrow::Cow::Borrowed(id),
		}
	}

	/// Restores the `!di`/`!dd` mapping of any record in `reclaimed` that exists
	/// again — i.e. was re-created concurrently with the sweep chunk that
	/// reclaimed its mapping, adopting the old id from its pre-reclaim snapshot.
	///
	/// [`TableDocIds::restore`] claims the forward mapping with a put-if-absent,
	/// so a record that instead re-acquired a *fresh* id after the reclaim is
	/// left untouched. Best-effort like the sweep itself: on repeated conflicts
	/// the remaining repairs are abandoned with a warning, and the affected
	/// record self-heals on its next indexed write.
	async fn repair_recreated_doc_ids(
		&self,
		docs: &TableDocIds,
		reclaimed: &[(RecordIdKey, DocId)],
	) -> Result<()> {
		if reclaimed.is_empty() {
			return Ok(());
		}
		let mut retries = 0usize;
		loop {
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let record_keys: Vec<record::RecordKey> =
				reclaimed.iter().map(|(id, _)| self.record_key(id)).collect();
			let records = catch!(tx, tx.get_many_key(record_keys, None).await);
			let mut restored = 0usize;
			for ((id, doc_id), existing) in reclaimed.iter().zip(records) {
				if existing.is_some() && catch!(tx, docs.restore(&tx, id, *doc_id).await) {
					restored += 1;
				}
			}
			if restored == 0 {
				tx.cancel().await?;
				return Ok(());
			}
			if !self
				.commit_and_retryable_conflict(
					&tx,
					"transient conflict repairing re-created doc-ID mappings, retrying",
				)
				.await?
			{
				warn!(
					index = %self.ix.name,
					table = %self.ix.table_name,
					restored,
					"restored doc-ID mappings of records re-created concurrently \
					 with the deferred reclaim sweep"
				);
				return Ok(());
			}
			retries += 1;
			if retries > DOC_ID_RECLAIM_MAX_RETRIES {
				warn!(
					index = %self.ix.name,
					table = %self.ix.table_name,
					"giving up doc-ID mapping repair after repeated conflicts; \
					 affected records re-index on their next write"
				);
				return Ok(());
			}
		}
	}

	/// Replay legacy `!ig` mutations that were committed before this protocol.
	async fn index_appending_range(
		&self,
		ctx: &FrozenContext,
		tx: &Transaction,
		keys: Vec<Vec<u8>>,
		count: &mut usize,
	) -> Result<()> {
		let mut rc = false;
		let mut stack = TreeStack::new();
		let fulltext_index =
			IndexOperation::create_fulltext_index(ctx, self.ix_key.ns, self.ix_key.db, &self.ix)
				.await?;
		for k in keys {
			// Replay analysis is synchronous CPU work; yield so a long batch
			// cannot starve the runtime worker.
			yield_now!();
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(Some(*count))?;
			let ig = IndexAppending::decode_key(&k)?;
			if let Some(appending) = tx.get_key(&ig, None).await? {
				let rid_key = self
					.apply_appending(ctx, &mut stack, &fulltext_index, appending, &mut rc)
					.await?;
				tx.del_key(&ig).await?;

				// We can delete the ip record if any
				let ip = self.ikb.new_ip_key(rid_key);
				tx.del_key(&ip).await?;
			}

			*count += 1;
		}
		// Trigger compaction if needed.
		self.check_index_compaction(tx, &mut rc).await?;
		// We're done.
		Ok(())
	}

	/// Replay durable `!bg` mutations and delete their `!bp` primary markers.
	async fn index_durable_appending_range(
		&self,
		ctx: &FrozenContext,
		tx: &Transaction,
		keys: Vec<Vec<u8>>,
		count: &mut usize,
	) -> Result<()> {
		let mut rc = false;
		let mut stack = TreeStack::new();
		let fulltext_index =
			IndexOperation::create_fulltext_index(ctx, self.ix_key.ns, self.ix_key.db, &self.ix)
				.await?;
		for k in keys {
			// Replay analysis is synchronous CPU work; yield so a long batch
			// cannot starve the runtime worker.
			yield_now!();
			if self.is_aborted().await {
				return Ok(());
			}
			self.is_beyond_threshold(Some(*count))?;
			let bg = Bg::decode_key(&k)?;
			if let Some(appending) = tx.get_key(&bg, None).await? {
				let rid_key = self
					.apply_appending(ctx, &mut stack, &fulltext_index, appending, &mut rc)
					.await?;
				tx.del_key(&bg).await?;
				let bp = self.ikb.new_bp_key(bg.generation, &rid_key);
				tx.del_key(&bp).await?;
				tx.del_key(&self.ikb.new_br_key(bg.generation, bg.ticket)).await?;
			}
			*count += 1;
		}
		self.check_index_compaction(tx, &mut rc).await?;
		Ok(())
	}

	async fn check_index_compaction(&self, tx: &Transaction, rc: &mut bool) -> Result<()> {
		if !*rc {
			return Ok(());
		}
		IndexOperation::compaction_trigger(&self.ikb, tx, self.ctx.node_id()).await?;
		*rc = false;
		Ok(())
	}
}
