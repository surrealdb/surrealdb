use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::Result;
use chrono::Utc;
use common::time::sleep;
use reblessive::TreeStack;
use uuid::Uuid;
use web_time::Instant;

use super::builder::Building;
use super::{
	Appending, BUILD_CLOSING_SLEEP, BuildGeneration, ExistingPrimaryAppending, IndexBuildPhase,
	IndexBuildReportStatus, LEGACY_BATCH_ID, PrimaryAppendingTicket,
};
use crate::catalog::providers::{NodeProvider, TableProvider};
use crate::catalog::{Index, Record};
use crate::ctx::FrozenContext;
use crate::doc::{CursorDoc, Document};
use crate::exe::FlowResultExt as _;
use crate::idx::docids::{DocId, TableDocIds};
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::index::IndexOperation;
use crate::idx::{IndexKeyBase, PreviousBuildPrimaryKey};
use crate::key::schema::{
	BuildAppendKey, BuildReservationKey, DocLookupKey, DocLookupPrefix, DocPendingKey,
	DocPendingPrefix, IndexAppendKey, RecordKey, RecordPrefix,
};
use crate::key::{KVKey, KVKeyDecode, KVSubspace, KVValue, Key, Resumable};
use crate::kvs::{
	DatastoreError, Direction, INDEXING_BATCH_SIZE, Transaction, Val,
	is_retryable_transaction_conflict,
};
use crate::legacy::analyzer_function::LegacyAnalyzerFunction;
use crate::val::{Array, Number, Object, RecordId, RecordIdKey, RecordIdentity, Value};

/// Maximum consecutive commit-conflict retries for one deferred doc-ID reclaim
/// sweep chunk (and for its repair pass) before the sweep gives up.
///
/// The sweep runs after the build is durably `Online` and its `!dp` markers are
/// durable, so giving up loses nothing — leftover markers are drained by the
/// next doc-ID index build on the table. The bound exists so a conflict storm
/// cannot wedge the finished build task (and, for a blocking `DEFINE INDEX`,
/// the statement waiting on it).
const DOC_ID_RECLAIM_MAX_RETRIES: usize = 10;

/// Maximum consecutive commit-conflict retries for one page of the rebuild of a
/// generation's primary-append markers before the build gives up.
///
/// Unlike the reclaim sweep this cannot leave its work for a later build: the
/// initial scan about to run is the reader the markers are for, and a page
/// left unwritten would have it baseline records as though they had no queued
/// mutation. Giving up therefore fails the build, which a rebuild recovers from
/// by starting a fresh generation.
const PRIMARY_APPEND_REBUILD_MAX_RETRIES: usize = 10;

/// Most ids a pending-reclaim marker in the previous spelling is read as.
///
/// That spelling drops a nested number's variant, so the marker stands for
/// every id that differs only there — three per number, an integer, a float
/// and a decimal of one value. The bound covers an id with three numbers in
/// full and keeps a marker with more from costing a read per combination.
const LEGACY_MARKER_MAX_READINGS: usize = 27;

struct InitialIndexValue<'a> {
	rid: &'a RecordId,
	opt_values: Option<Vec<Value>>,
	count_cond_match: Option<(bool, bool)>,
}

/// How far a COUNT build's merge of queued old states has reached, carried
/// from one initial-scan batch to the next.
///
/// Cloned with the batch it belongs to, so a batch that is retried starts again
/// from the state before it.
#[derive(Clone, Debug, Default)]
pub(super) struct CountPrimaryProgress {
	/// The last record the merge has covered; `None` before the first batch.
	cursor: Option<RecordIdKey>,
	/// Markers met in a span before the one holding their record, by record,
	/// to be taken up in that span. Only a marker an older node spelled under
	/// the index format can sit there.
	early: BTreeMap<RecordIdentity, PrimaryAppendingTicket>,
	/// Records whose span read an older node's marker for them that sorts
	/// after it, which the merge skips when it gets there.
	late_read: BTreeSet<RecordIdentity>,
}

impl CountPrimaryProgress {
	/// Progress resuming after `cursor`, the last record a checkpoint covered.
	///
	/// Nothing is set aside or read yet: a scan starts by rebuilding every
	/// marker at its record's own key, so none is left in another record's span.
	/// A resume that would leave an older node's marker unmet for a record at or
	/// before the cursor rescans instead
	/// ([`Building::resume_strands_an_older_marker`]).
	pub(super) fn resuming_at(cursor: Option<RecordIdKey>) -> Self {
		Self {
			cursor,
			..Self::default()
		}
	}

	/// Takes the markers set aside for records up to `through`, or every one
	/// once the scan has no live record left.
	fn take_due(
		&mut self,
		through: Option<&RecordIdentity>,
	) -> Vec<(RecordIdentity, PrimaryAppendingTicket)> {
		match through {
			Some(through) => self.early.extract_if(..=through, |_, _| true).collect(),
			None => std::mem::take(&mut self.early).into_iter().collect(),
		}
	}
}

struct CountPrimaryAppendingScan<'a> {
	lookup_tx: &'a Transaction,
	progress: &'a mut CountPrimaryProgress,
	through: Option<&'a RecordIdKey>,
	live_ids: &'a BTreeSet<RecordIdentity>,
	/// The keys of the older nodes' markers the batch's scan read, by record.
	older_markers: &'a [(RecordIdentity, PreviousBuildPrimaryKey)],
	initial_count: usize,
}

/// Whether mutation `a` of a record was queued no later than its mutation `b`.
///
/// A record's first queued mutation is the one with the lowest ticket, and its
/// old state is the record before the queue: a writer draws its ticket after
/// its snapshot and before it commits, and tickets are drawn in order, so no
/// other queued mutation of the record committed before that writer's
/// snapshot. That holds while every writer draws its tickets from the
/// generation's one counter, which the build checks before it publishes
/// (`ensure_single_ticket_allocator`).
fn queued_no_later(a: PrimaryAppendingTicket, b: PrimaryAppendingTicket) -> bool {
	(a.ticket, a.mutation_seq) <= (b.ticket, b.mutation_seq)
}

/// The predicate results a record baselined from its queued mutation's old
/// state carries: no state before it, and the old state's own result.
fn baselined_match(count_cond_match: Option<(bool, bool)>) -> Option<(bool, bool)> {
	count_cond_match.map(|(old_matches, _)| (false, old_matches))
}

/// A `!bp` marker whose queued mutation has confirmed the marker's record,
/// with that mutation.
struct Marker {
	ptr: PrimaryAppendingTicket,
	appending: Appending,
}

impl Marker {
	/// The earlier of two markers for one record.
	fn earlier(self, other: Self) -> Self {
		if queued_no_later(self.ptr, other.ptr) {
			self
		} else {
			other
		}
	}
}

/// When the merge meets a marker, against the span that holds its record's
/// own key.
enum Met {
	/// In a span before its record's.
	Before,
	/// In its record's span.
	In,
	/// In a span after its record's.
	After,
}

impl Met {
	/// Where `record` sorts against the span after `after` through `through`,
	/// which runs to the end when `through` is `None`.
	fn place(
		record: &RecordIdentity,
		after: Option<&RecordIdentity>,
		through: Option<&RecordIdentity>,
	) -> Self {
		if through.is_some_and(|through| record > through) {
			Met::Before
		} else if after.is_some_and(|after| record <= after) {
			Met::After
		} else {
			Met::In
		}
	}
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
				let br = BuildReservationKey::decode_key(&key)?;
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
		// Reservations sort by generation, so the stale span is every
		// reservation whose generation is below `below`.
		let rng = self.ikb.new_br_range_below(below)?;
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
				let br = BuildReservationKey::decode_key(&key)?;
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
	///
	/// A node with no cluster row is not live: it has been reaped, so its
	/// reservation is as expired as one an archived node still holds.
	async fn reservation_node_is_live(&self, tx: &Transaction, node: Uuid) -> Result<bool> {
		Ok(tx.get_node(node).await?.is_some_and(|node| node.is_active()))
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
		count_progress: &mut Option<CountPrimaryProgress>,
	) -> Result<usize> {
		let mut rc = false;
		let mut count = 0;
		let mut live_ids = BTreeSet::new();
		let mut last_live_id = None;
		let mut older_markers = Vec::new();
		let generation = self.build_generation.load(Ordering::Acquire);
		// Each record's key, decoded once for the scan and the older markers.
		let keys =
			values.iter().map(|(k, _)| RecordKey::decode_key(k)).collect::<Result<Vec<_>>>()?;
		let mut stack = TreeStack::new();
		let fulltext_index =
			IndexOperation::create_fulltext_index(ctx, self.ix_key.ns, self.ix_key.db, &self.ix)
				.await?;
		let lookup_tx = self.new_read_tx().await?;
		// COUNT WHERE predicate, already parsed once when the build started.
		let count_cond_expr: Option<crate::expr::Expr> =
			self.ix.count_cond.as_ref().map(|c| c.0.clone());
		let result = async {
			// A record is baselined from its first queued mutation, which an
			// older node may have marked under its own spelling.
			let mut older_held = if generation != 0 {
				self.older_markers_held(&lookup_tx, generation, values, &keys).await?
			} else {
				Vec::new()
			};
			// Index the records.
			for (i, ((_, v), key)) in values.iter().zip(keys).enumerate() {
				// Per-record analysis (deserialization, tokenization, filters)
				// is synchronous CPU work with no other guaranteed await; yield
				// so a long batch cannot starve the runtime worker.
				yield_now!();
				if self.is_aborted().await {
					return Ok(count);
				}
				self.is_beyond_threshold(Some(initial_count + count))?;
				// Parse the value.
				let val: Record = revision::from_slice(v.as_slice())?;
				let rid: Arc<RecordId> = RecordId {
					table: key.tb.into_owned(),
					key: key.id.into_owned(),
				}
				.into();
				if count_progress.is_some() {
					live_ids.insert(RecordIdentity(rid.key.clone()));
					last_live_id = Some(rid.key.clone());
				}

				// An older node's marker counts once its queued mutation names
				// the record. A COUNT build's merge meets it again, so it notes
				// where.
				let older = match older_held.get_mut(i).and_then(Option::take) {
					Some((key, ptr)) => {
						let marker =
							self.confirmed_marker(&lookup_tx, generation, ptr, &rid.key).await?;
						if marker.is_some() && count_progress.is_some() {
							older_markers.push((RecordIdentity(rid.key.clone()), key));
						}
						marker
					}
					None => None,
				};
				// Is there already a queued update for this record?
				let (opt_values, count_cond_match) = if let Some(a) = self
					.check_existing_primary_appending(
						&lookup_tx,
						&rid.key,
						older,
						v1_appending_sentinel,
					)
					.await?
				{
					(a.old_values, baselined_match(a.count_cond_match))
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
			if let Some(progress) = count_progress.as_mut()
				&& let Some(through) = last_live_id.as_ref()
			{
				count += self
					.index_missing_count_primary_appendings(
						ctx,
						CountPrimaryAppendingScan {
							lookup_tx: &lookup_tx,
							progress,
							through: Some(through),
							live_ids: &live_ids,
							older_markers: &older_markers,
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
		count_progress: &mut Option<CountPrimaryProgress>,
		initial_count: usize,
	) -> Result<usize> {
		let Some(progress) = count_progress.as_mut() else {
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
					progress,
					through: None,
					live_ids: &live_ids,
					older_markers: &[],
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
		let az_fn = LegacyAnalyzerFunction::new(ctx, &self.opt);
		let mut io = IndexOperation::new(
			ctx,
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
				.enter(|stk| io.compute_fulltext_with_index(stk, &az_fn, fulltext_index, rc))
				.finish()
				.await
		} else {
			stack.enter(|stk| io.compute(stk, &az_fn, rc)).finish().await
		}
	}

	/// Merge orphaned COUNT primary markers into the initial baseline.
	///
	/// Durable `!bp` markers record the first queued mutation for each record
	/// while the initial scan is running. A COUNT build must index the queued
	/// old state even when the live record scan no longer sees the record, so
	/// later replay deltas are applied against the same baseline.
	///
	/// Each call covers the span of keys from the last record the merge covered
	/// to the last live record of the batch just scanned, and settles the records
	/// whose own keys it holds: the scan has baselined the live ones, and a gone
	/// one is baselined from the marker at its own key. Either takes the earlier
	/// of the mutations that marker and an older node's marker for the record
	/// point at. An older node spells the id under the index format, so its
	/// marker sits at another key, which can fall in a span before or after its
	/// record's:
	///
	/// - one met before its record's span is set aside and judged there;
	/// - one in its record's span counts only while the record is gone and its own key holds no
	///   marker for it;
	/// - one met after its record's span is skipped if that span read it, or if the record's own
	///   key marks an earlier mutation, which that span read or which came after it. Otherwise the
	///   marker came after the span, which baselined the record as it stood, or the record was gone
	///   from the span: it counts only if the record is gone when it is met. That stands in for the
	///   record's presence in its span, so a record that appears or goes in between is miscounted
	///   by one.
	///
	/// A call without `through` covers the markers after the last span, takes up
	/// every marker set aside and leaves nothing for a later span.
	///
	/// A marker is met only if it is written before the merge passes its key. One
	/// sorting before its record that is written after that is read by the scan
	/// when the record is live in its span, and by the record's own marker when it
	/// has one; a record only older nodes marked that is gone from its span by
	/// then is not counted.
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
		let after = scan.progress.cursor.clone().map(RecordIdentity);
		let through = scan.through.map(|t| RecordIdentity(t.clone()));
		// An older marker this span reads is met again if it sorts past the span.
		let span_end =
			scan.through.map(|t| self.ikb.new_bp_key(generation, t).encode_key()).transpose()?;
		let past_span = |key: &[u8]| span_end.as_ref().is_some_and(|end| key > &**end);
		for (record, key) in scan.older_markers {
			if past_span(key.as_bytes()) {
				scan.progress.late_read.insert(record.clone());
			}
		}
		let mut count = 0;
		// Markers set aside earlier whose record this span holds.
		for (record, ptr) in scan.progress.take_due(through.as_ref()) {
			yield_now!();
			if self.is_aborted().await {
				return Ok(count);
			}
			self.is_beyond_threshold(Some(scan.initial_count + count))?;
			if self.settled_in_span(&scan, generation, &record).await? {
				continue;
			}
			let appending = self.queued_appending(scan.lookup_tx, generation, ptr).await?;
			self.index_queued_baseline(ctx, stack, rc, appending).await?;
			count += 1;
		}
		let range =
			self.ikb.new_bp_span_range(generation, scan.progress.cursor.as_ref(), scan.through)?;
		let mut next = (range.start() < range.end()).then_some(range);
		while let Some(rng) = next {
			if self.is_aborted().await {
				return Ok(count);
			}
			let batch =
				scan.lookup_tx.batch_keys_vals(rng.clone(), INDEXING_BATCH_SIZE, None).await?;
			// A full page resumes just after the last key it returned; a short
			// page means the span is drained.
			next = match (&batch.next, batch.result.last()) {
				(Some(_), Some((k, _))) => Some(rng.resume_after(k, Direction::Forward)),
				_ => None,
			};
			for (key, val) in batch.result {
				// Baseline indexing is synchronous CPU work; yield so a long
				// batch cannot starve the runtime worker.
				yield_now!();
				if self.is_aborted().await {
					return Ok(count);
				}
				self.is_beyond_threshold(Some(scan.initial_count + count))?;
				// The record comes from the queued mutation, never from the
				// marker's key: a key an older node spelled under the index
				// format need not decode, and one that does can name another
				// record. The mutation carries the id losslessly.
				let ptr = PrimaryAppendingTicket::kv_decode_value(&val, ())?;
				let appending = self.queued_appending(scan.lookup_tx, generation, ptr).await?;
				let record = RecordIdentity(appending.id.clone());
				// A record the batch just scanned was baselined by the scan.
				if scan.live_ids.contains(&record) {
					continue;
				}
				let own_key = self.ikb.new_bp_key(generation, &appending.id).encode_key()?;
				let appending = if *own_key == *key {
					// The record's own marker, unless an older node marked an
					// earlier mutation of it.
					let own = Marker {
						ptr,
						appending,
					};
					match self.older_marker(scan.lookup_tx, generation, &record.0).await? {
						Some((older_key, older)) => {
							if past_span(older_key.as_bytes()) {
								scan.progress.late_read.insert(record);
							}
							own.earlier(older).appending
						}
						None => own.appending,
					}
				} else {
					// A marker an older node wrote at another key than its
					// record's, judged by where its record is.
					match Met::place(&record, after.as_ref(), through.as_ref()) {
						Met::Before => {
							scan.progress.early.entry(record).or_insert(ptr);
							continue;
						}
						Met::In => {
							if self.settled_in_span(&scan, generation, &record).await? {
								continue;
							}
						}
						Met::After => {
							// It is met only here, so the record's entry is spent
							// however it is judged.
							if scan.progress.late_read.remove(&record) {
								continue;
							}
							let own =
								self.own_marker(scan.lookup_tx, generation, &record.0).await?;
							if own.is_some_and(|own| queued_no_later(own.ptr, ptr))
								|| scan
									.lookup_tx
									.exists_key(&self.record_key(&record.0), None)
									.await?
							{
								continue;
							}
						}
					}
					appending
				};
				self.index_queued_baseline(ctx, stack, rc, appending).await?;
				count += 1;
			}
		}
		if let Some(last) = scan.through {
			scan.progress.cursor = Some(last.clone());
		}
		Ok(count)
	}

	/// The queued mutation a marker points at. The initial scan never consumes
	/// one, so a marker naming a missing mutation is corruption.
	async fn queued_appending(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		ptr: PrimaryAppendingTicket,
	) -> Result<Appending> {
		let bg = self.ikb.new_bg_key(generation, ptr.ticket, ptr.mutation_seq);
		let Some(appending) = tx.get_key(&bg, None).await? else {
			return Err(
				DatastoreError::CorruptedIndex("Durable appending record is missing").into()
			);
		};
		Ok(appending)
	}

	/// The marker at `record`'s own key, with the queued mutation it points at,
	/// when that mutation is `record`'s.
	///
	/// A marker an older node wrote for another record can sit there — the older
	/// spelling of `[1, NONE]` is `[1]`'s key under this release — so the marker
	/// at the key counts only once its queued mutation confirms the record.
	async fn own_marker(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		record: &RecordIdKey,
	) -> Result<Option<Marker>> {
		let Some(ptr) = tx.get_key(&self.ikb.new_bp_key(generation, record), None).await? else {
			return Ok(None);
		};
		self.confirmed_marker(tx, generation, ptr, record).await
	}

	/// The marker an older node left for `record` under its spelling, with the
	/// key it sits at.
	///
	/// Only an id holding a number is spelled differently there, and the key can
	/// be another record's, so the marker counts only once its queued mutation
	/// confirms the record.
	async fn older_marker(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		record: &RecordIdKey,
	) -> Result<Option<(PreviousBuildPrimaryKey, Marker)>> {
		let Some(key) = self.ikb.new_bp_key_in_previous_spelling(generation, record)? else {
			return Ok(None);
		};
		let Some(ptr) = tx.get_key(&key, None).await? else {
			return Ok(None);
		};
		Ok(self.confirmed_marker(tx, generation, ptr, record).await?.map(|marker| (key, marker)))
	}

	/// Whether `record`, whose own key the span holds, is settled there without
	/// an older node's marker: the scan baselined it, or its own key holds a
	/// marker for it, which takes up the older one with it.
	async fn settled_in_span(
		&self,
		scan: &CountPrimaryAppendingScan<'_>,
		generation: BuildGeneration,
		record: &RecordIdentity,
	) -> Result<bool> {
		Ok(scan.live_ids.contains(record)
			|| self.own_marker(scan.lookup_tx, generation, &record.0).await?.is_some())
	}

	/// The markers an older node may have left for the batch's records under
	/// its spelling, read in one go and in the batch's order.
	///
	/// Each is the key that spelling gives the record, for an id it spells
	/// differently, and the marker held there. The mutation a marker points at
	/// is not read, so it can still name another record.
	async fn older_markers_held(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		values: &[(Vec<u8>, Val)],
		keys: &[RecordKey<'_>],
	) -> Result<Vec<Option<(PreviousBuildPrimaryKey, PrimaryAppendingTicket)>>> {
		let spelling = self.ikb.previous_bp_spelling(generation)?;
		// A record's key ends with its id in this release's spelling.
		let prefix = RecordPrefix {
			ns: self.ix_key.ns,
			db: self.ix_key.db,
			tb: Cow::Borrowed(self.ikb.table()),
		}
		.encode_bound()?
		.len();
		let mut older = Vec::new();
		let mut spelled = Vec::with_capacity(values.len());
		for ((k, _), key) in values.iter().zip(keys) {
			let found = spelling.key(&key.id, &k[prefix..])?;
			spelled.push(found.is_some());
			older.extend(found);
		}
		if older.is_empty() {
			return Ok(vec![None; values.len()]);
		}
		let held = tx.get_many_key(older.iter().collect(), None).await?;
		let mut held = older.into_iter().zip(held).map(|(key, ptr)| ptr.map(|ptr| (key, ptr)));
		Ok(spelled
			.into_iter()
			.map(|spelled| {
				if spelled {
					held.next().flatten()
				} else {
					None
				}
			})
			.collect())
	}

	/// The marker pointing at `ptr`, if its queued mutation names `record`.
	async fn confirmed_marker(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
		ptr: PrimaryAppendingTicket,
		record: &RecordIdKey,
	) -> Result<Option<Marker>> {
		let appending = self.queued_appending(tx, generation, ptr).await?;
		Ok(appending.id.addresses_same_record(record).then_some(Marker {
			ptr,
			appending,
		}))
	}

	/// Indexes the old state a record's queued mutation carries, as the
	/// record's baseline.
	async fn index_queued_baseline(
		&self,
		ctx: &FrozenContext,
		stack: &mut TreeStack,
		rc: &mut bool,
		appending: Appending,
	) -> Result<()> {
		let rid = RecordId {
			table: self.ikb.table().clone(),
			key: appending.id,
		};
		let count_cond_match = baselined_match(appending.count_cond_match);
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
		.await
	}

	/// Whether resuming a COUNT build's scan after `cursor` would leave an older
	/// node's marker unmet: one that sorts after the cursor's own key but names a
	/// record at or before it.
	///
	/// The merge takes such a marker up only when a later span meets it, and the
	/// rebuild that starts a resumed scan moves every marker to its record's own
	/// key, which is behind the cursor for this one. Whether its record was live
	/// when its span was scanned is not recorded, so any such marker counts. A
	/// marker an older node writes after the check needs no span: its record's
	/// span already saw the record as it stood before that write.
	pub(super) async fn resume_strands_an_older_marker(
		&self,
		generation: BuildGeneration,
		cursor: &RecordIdKey,
	) -> Result<bool> {
		let tx = self.new_read_tx().await?;
		let result = async {
			let passed = RecordIdentity(cursor.clone());
			let range = self.ikb.new_bp_span_range(generation, Some(cursor), None)?;
			let mut next = (range.start() < range.end()).then_some(range);
			while let Some(rng) = next {
				let batch = tx.batch_keys_vals(rng.clone(), INDEXING_BATCH_SIZE, None).await?;
				next = match (&batch.next, batch.result.last()) {
					(Some(_), Some((k, _))) => Some(rng.resume_after(k, Direction::Forward)),
					_ => None,
				};
				let queued = batch
					.result
					.iter()
					.map(|(_, val)| {
						let ptr = PrimaryAppendingTicket::kv_decode_value(val, ())?;
						Ok(self.ikb.new_bg_key(generation, ptr.ticket, ptr.mutation_seq))
					})
					.collect::<Result<Vec<_>>>()?;
				for appending in tx.get_many_key(queued, None).await? {
					let Some(appending) = appending else {
						return Err(DatastoreError::CorruptedIndex(
							"Durable appending record is missing",
						)
						.into());
					};
					if RecordIdentity(appending.id) <= passed {
						return Ok(true);
					}
				}
			}
			Ok(false)
		}
		.await;
		let cancel_result = tx.cancel().await;
		match result {
			Ok(strands) => {
				cancel_result?;
				Ok(strands)
			}
			Err(err) => {
				let _ = cancel_result;
				Err(err)
			}
		}
	}

	/// Rebuilds this generation's `!bp` markers from its queued mutations.
	///
	/// A marker names a record's first queued mutation, so the markers are an
	/// index over `!bg`, and `!bg` is keyed by ticket and carries the record id
	/// losslessly in its value. Rebuilding them from there is what makes them
	/// correct whatever spelling they were written in. A previous release spelled
	/// the record id under the index format, which is not injective and whose
	/// bytes for one record can equal this release's bytes for another, so no
	/// marker already on disk can be trusted to sit at the key its record's
	/// marker belongs at — and a writer that found such a key occupied recorded no
	/// marker of its own, which only `!bg` still knows about.
	///
	/// The generation's markers are cleared, then each record gets one from its
	/// earliest queued mutation. A write admitted meanwhile takes a later ticket
	/// than every mutation already queued, so where one has placed a marker for a
	/// record whose earlier mutation is yet to be reached, the earlier one replaces
	/// it. Both steps go a page at a time, each page in a transaction of its own,
	/// so neither is bounded by how much one transaction may write or delete — a
	/// build can hold a marker for every record written while it scans. Runs at
	/// the top of every initial scan, before any reader of the markers, renewing
	/// the build lease on every page so a long rebuild keeps ownership.
	pub(super) async fn rebuild_primary_appendings(&self) -> Result<()> {
		let generation = self.build_generation.load(Ordering::Acquire);
		if generation == 0 {
			return Ok(());
		}
		// Clear first. A marker in the previous spelling can occupy the very key
		// another record's marker belongs at, so it has to be gone before any
		// marker is written. The clear pages rather than deleting the range in
		// one call, which a backend may cap. Pages are independent of the writes
		// admitted between them: a page can delete a marker such a write placed
		// ahead of the clear, and one placed behind it is left, and in both
		// cases the pass over the queued mutations below writes the marker the
		// record should have.
		let mut retries = 0usize;
		let mut next = Some(self.ikb.new_bp_range(generation)?);
		while let Some(rng) = next.clone() {
			if self.is_aborted().await {
				return Ok(());
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			catch!(
				tx,
				self.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building]).await
			);
			let batch = catch!(tx, tx.batch_keys(rng.clone(), INDEXING_BATCH_SIZE, None).await);
			// Where the next page starts, taken up only once this one commits.
			let after = batch
				.next
				.and(batch.result.last())
				.map(|last| rng.resume_after(last, Direction::Forward));
			for key in &batch.result {
				catch!(tx, tx.del(Key::from(key.as_slice())).await);
			}
			if self
				.commit_and_retryable_conflict(
					&tx,
					"transient conflict clearing primary-append markers, retrying",
				)
				.await?
			{
				retries += 1;
				if retries > PRIMARY_APPEND_REBUILD_MAX_RETRIES {
					return Err(self.primary_append_rebuild_failed());
				}
				continue;
			}
			retries = 0;
			next = after;
		}
		// Rebuild from the queued mutations, earliest first: `!bg` ascends by
		// ticket, so the first entry met for a record is its earliest mutation.
		let mut retries = 0usize;
		let mut next = Some(self.ikb.new_bg_range(generation)?);
		while let Some(rng) = next.clone() {
			if self.is_aborted().await {
				return Ok(());
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			catch!(
				tx,
				self.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building]).await
			);
			let batch =
				catch!(tx, tx.batch_keys_vals(rng.clone(), INDEXING_BATCH_SIZE, None).await);
			// Where the next page starts, taken up only once this one commits: a
			// conflict retries this page rather than stepping past it.
			let after = match (&batch.next, batch.result.last()) {
				(Some(_), Some((k, _))) => Some(rng.resume_after(k, Direction::Forward)),
				_ => None,
			};
			// One marker per record on this page, from its earliest mutation here.
			let mut seen = HashSet::new();
			let mut firsts = Vec::new();
			for (key, val) in &batch.result {
				let bg = catch!(tx, BuildAppendKey::decode_key(key));
				let appending = catch!(tx, Appending::kv_decode_value(val, ()));
				if !seen.insert(RecordIdentity(appending.id.clone())) {
					continue;
				}
				let ptr = PrimaryAppendingTicket {
					ticket: bg.ticket,
					mutation_seq: bg.mutation_seq,
				};
				firsts.push((appending.id, ptr));
			}
			let markers =
				firsts.iter().map(|(id, _)| self.ikb.new_bp_key(generation, id)).collect();
			let held = catch!(tx, tx.get_many_key(markers, None).await);
			for ((id, ptr), held) in firsts.iter().zip(held) {
				// A marker already there was written by an earlier page, naming an
				// earlier mutation of this record, or by a write admitted since the
				// clear, naming a later one. Only the earlier stands.
				if held.is_some_and(|held| queued_no_later(held, *ptr)) {
					continue;
				}
				catch!(tx, tx.set_key(&self.ikb.new_bp_key(generation, id), ptr).await);
			}
			if self
				.commit_and_retryable_conflict(
					&tx,
					"transient conflict rebuilding primary-append markers, retrying",
				)
				.await?
			{
				retries += 1;
				if retries > PRIMARY_APPEND_REBUILD_MAX_RETRIES {
					return Err(self.primary_append_rebuild_failed());
				}
				continue;
			}
			retries = 0;
			next = after;
		}
		Ok(())
	}

	/// The error a build fails with when its primary-append markers cannot be
	/// rebuilt; a rebuild of the index starts a fresh generation, which has none.
	fn primary_append_rebuild_failed(&self) -> anyhow::Error {
		DatastoreError::QueryNotExecuted {
			message: format!(
				"could not rebuild the primary-append markers of index `{}` on table `{}`; rebuild the index",
				self.ix.name, self.ix.table_name
			),
		}
		.into()
	}

	/// Look up an existing per-record appending marker before initial indexing.
	async fn check_existing_primary_appending(
		&self,
		lookup_tx: &Transaction,
		id_key: &RecordIdKey,
		older: Option<Marker>,
		v1_appending_sentinel: &mut bool,
	) -> Result<Option<Appending>> {
		match self.load_existing_primary_appending(lookup_tx, id_key, older).await? {
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
	/// Durable `!bp` markers point to generation-scoped `!bg` entries, and a
	/// marker counts only when its mutation names `id_key`: an older node spells
	/// the id under the index format, whose bytes for another record can equal
	/// this release's bytes for `id_key`. `older` is the marker such a node left
	/// for `id_key` under its own spelling, which the scan reads, and the earlier
	/// of it and the marker at `id_key`'s own key is the one returned.
	/// Legacy `!ip` markers point to `!ig` entries unless they are old v1 markers
	/// with no batch id, which cannot be resolved safely and are cleaned up.
	async fn load_existing_primary_appending(
		&self,
		tx: &Transaction,
		id_key: &RecordIdKey,
		older: Option<Marker>,
	) -> Result<ExistingPrimaryAppending> {
		let generation = self.build_generation.load(Ordering::Acquire);
		if generation != 0 {
			let own = self.own_marker(tx, generation, id_key).await?;
			if let Some(first) = own.into_iter().chain(older).reduce(Marker::earlier) {
				return Ok(ExistingPrimaryAppending::Appending(first.appending));
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
			&& self.ix.uses_shared_doc_ids();
		let rid = RecordId {
			table: self.ikb.table().clone(),
			key: rid_key.clone(),
		};
		let az_fn = LegacyAnalyzerFunction::new(ctx, &self.opt);
		let mut io = IndexOperation::new(
			ctx,
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
				.enter(|stk| io.compute_fulltext_with_index(stk, &az_fn, fulltext_index, rc))
				.finish()
				.await?;
		} else {
			stack.enter(|stk| io.compute(stk, &az_fn, rc)).finish().await?;
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
				let dp = DocPendingKey::new(
					self.ix_key.ns,
					self.ix_key.db,
					Cow::Borrowed(self.ikb.table()),
					RecordIdentity(rid_key.clone()),
				);
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
			if other.index_id == self.ix.index_id || !other.uses_shared_doc_ids() {
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
		let dp = DocPendingKey::new(
			self.ix_key.ns,
			self.ix_key.db,
			Cow::Borrowed(self.ikb.table()),
			RecordIdentity(id.clone()),
		);
		tx.set_key(&dp, &()).await
	}

	/// The record a pending-reclaim marker names, when the marker was written by
	/// a release that spelled the id under the index format.
	///
	/// That spelling is not injective and does not decode as an identity, so the
	/// id cannot be read out of the key directly. It is still exactly how `!di`
	/// spells an id, so the marker's own suffix addresses that mapping, and the
	/// reverse mapping it names carries the id losslessly (see
	/// [`TableDocIds::record_spelled_as`]). Recovering it this way keeps the
	/// reclaim the marker was written for, rather than forfeiting it.
	///
	/// Returns `None` when the chain does not resolve — the mapping is already
	/// gone, or the space is torn — in which case there is nothing left to
	/// reclaim and the marker is simply consumed.
	async fn record_named_by_legacy_marker(
		&self,
		tx: &Transaction,
		marker: &[u8],
	) -> Result<Option<RecordIdKey>> {
		let table = self.ikb.table();
		let pending = DocPendingPrefix::new(self.ix_key.ns, self.ix_key.db, Cow::Borrowed(table))
			.encode_bound()?;
		let Some(spelling) = marker.strip_prefix(&*pending) else {
			return Ok(None);
		};
		TableDocIds::new(self.ix_key.ns, self.ix_key.db, table.clone())
			.record_spelled_as(tx, spelling)
			.await
	}

	/// Every record a pending-reclaim marker in the previous spelling can stand
	/// for, up to [`LEGACY_MARKER_MAX_READINGS`]: the id its bytes decode to under
	/// the index format, in each variant of every number nested in it.
	///
	/// The chain [`record_named_by_legacy_marker`](Self::record_named_by_legacy_marker)
	/// follows reaches whichever of those records holds the type-erased key now,
	/// which need not be the one marked: once another takes the key over, the
	/// marked record is reached only through its own injective mapping, under
	/// the variant it was written with.
	fn legacy_marker_readings(&self, marker: &[u8]) -> Result<Vec<RecordIdKey>> {
		let table = Cow::Borrowed(self.ikb.table());
		let pending =
			DocPendingPrefix::new(self.ix_key.ns, self.ix_key.db, table.clone()).encode_bound()?;
		let Some(spelling) = marker.strip_prefix(&*pending) else {
			return Ok(Vec::new());
		};
		let mut erased =
			DocLookupPrefix::new(self.ix_key.ns, self.ix_key.db, table).encode_bound()?.to_vec();
		erased.extend_from_slice(spelling);
		let Ok(di) = DocLookupKey::decode_key(&erased) else {
			return Ok(Vec::new());
		};
		Ok(numeric_variants(&di.id, LEGACY_MARKER_MAX_READINGS))
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
		if !self.ix.uses_shared_doc_ids() {
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
				DocPendingPrefix::new(
					self.ix_key.ns,
					self.ix_key.db,
					Cow::Borrowed(self.ikb.table())
				)
				.range()
			);
			let keys = catch!(tx, tx.keys(rng, INDEXING_BATCH_SIZE, 0, None).await);
			if keys.is_empty() {
				tx.cancel().await?;
				return Ok(());
			}
			// Which spelling a marker is in cannot be read off its bytes. The two
			// overlap: an id under this release's spelling encodes to the same
			// bytes as one under the previous release's followed by `NONE`, `NULL`
			// or a bool, because the tag after the number lands where its kind
			// would have been. So a marker that decodes may still name a different
			// record than the one it was written for, and a marker that does not
			// decode names one only the previous release's chain can reach.
			//
			// Both readings are taken, and neither has to be ruled out: the rule
			// this sweep applies — reclaim a mapping only once its record is gone —
			// is the right one for whichever record a candidate turns out to be. A
			// candidate that is really a live record keeps its mapping; one that is
			// absent should not be holding a mapping in any case.
			let mut ids: Vec<RecordIdKey> = Vec::with_capacity(keys.len());
			let mut seen = HashSet::with_capacity(keys.len());
			let mut take = |id: RecordIdKey, ids: &mut Vec<RecordIdKey>| {
				if seen.insert(RecordIdentity(id.clone())) {
					ids.push(id);
				}
			};
			for k in &keys {
				let decodes = match DocPendingKey::decode_key(k) {
					Ok(dp) => {
						let decoded = dp.id.0;
						// The index format differs from this one only over numbers,
						// so an id holding none encodes the same under both
						// spellings and the previous release's chain can only lead
						// back to the record just named. Only an id carrying a
						// number in a nested value has a second reading worth
						// paying two reads for.
						let has_second_reading = !decoded.hash_agrees_with_eq();
						take(decoded, &mut ids);
						if !has_second_reading {
							continue;
						}
						true
					}
					Err(_) => false,
				};
				// The previous release spelled the id the way `!di` still spells
				// one, so the marker's own bytes address that mapping and the
				// reverse mapping behind it names the record losslessly. Forfeiting
				// the reclaim instead would strand the mapping, which `REBUILD
				// INDEX` does not clear.
				if let Some(id) = catch!(tx, self.record_named_by_legacy_marker(&tx, k).await) {
					take(id, &mut ids);
				}
				// A marker this release's spelling cannot decode is in the previous
				// one, and the chain above reaches the record holding the erased key
				// now, which another record of the same spelling may have taken
				// over. So every record the spelling stands for is a reading too.
				if !decodes {
					for id in catch!(tx, self.legacy_marker_readings(k)) {
						take(id, &mut ids);
					}
				}
			}
			// One batched probe decides every record's fate in this chunk.
			let record_keys: Vec<RecordKey> = ids.iter().map(|id| self.record_key(id)).collect();
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
			}
			// The marker is consumed whether the mapping was reclaimed (record
			// absent), kept (record re-created during the build), already gone
			// (reclaimed by an earlier inline pass or sweep), or named a record
			// that could no longer be recovered at all. The key deleted is the one
			// the scan returned: a marker in the previous spelling re-encodes to
			// this release's, which is not the key on disk.
			for k in &keys {
				catch!(tx, tx.del(Key::from(k.as_slice())).await);
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
	fn record_key<'a>(&'a self, id: &'a RecordIdKey) -> RecordKey<'a> {
		RecordKey {
			ns: self.ix_key.ns,
			db: self.ix_key.db,
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
			let record_keys: Vec<RecordKey> =
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
			let ig = IndexAppendKey::decode_key(&k)?;
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
			let bg = BuildAppendKey::decode_key(&k)?;
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

/// `id` and every id that differs from it only in the variant of a number
/// nested in it, up to `cap` of them: the ids the index format spells alike.
fn numeric_variants(id: &RecordIdKey, cap: usize) -> Vec<RecordIdKey> {
	let rebuild = |v: Value| match v {
		Value::Array(a) => Some(RecordIdKey::Array(a)),
		Value::Object(o) => Some(RecordIdKey::Object(o)),
		_ => None,
	};
	match id {
		RecordIdKey::Array(a) => {
			value_variants(&Value::Array(a.clone()), cap).into_iter().filter_map(rebuild).collect()
		}
		RecordIdKey::Object(o) => {
			value_variants(&Value::Object(o.clone()), cap).into_iter().filter_map(rebuild).collect()
		}
		other => vec![other.clone()],
	}
}

/// [`numeric_variants`] over a value.
fn value_variants(value: &Value, cap: usize) -> Vec<Value> {
	match value {
		Value::Number(n) => number_variants(*n).into_iter().map(Value::Number).take(cap).collect(),
		Value::Array(a) => {
			let parts = a.0.iter().map(|v| value_variants(v, cap)).collect();
			capped_product(parts, cap).into_iter().map(|vs| Value::Array(Array(vs))).collect()
		}
		Value::Object(o) => {
			let keys: Vec<_> = o.0.iter().map(|(k, _)| k.clone()).collect();
			let parts = o.0.iter().map(|(_, v)| value_variants(v, cap)).collect();
			capped_product(parts, cap)
				.into_iter()
				.map(|vs| Value::Object(Object(keys.iter().cloned().zip(vs).collect())))
				.collect()
		}
		other => vec![other.clone()],
	}
}

/// The integer, float and decimal a number's value can be written as.
fn number_variants(n: Number) -> Vec<Number> {
	use rust_decimal::Decimal;
	use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
	let exact = match n {
		Number::Int(i) => Decimal::from(i),
		Number::Float(f) => match Decimal::from_f64(f) {
			Some(d) => d,
			None => return vec![n],
		},
		Number::Decimal(d) => d,
	};
	let mut out = Vec::with_capacity(3);
	if exact.is_integer()
		&& let Some(i) = exact.to_i64()
	{
		out.push(Number::Int(i));
	}
	if let Some(f) = exact.to_f64() {
		out.push(Number::Float(f));
	}
	out.push(Number::Decimal(exact));
	out
}

/// Every choice of one value per part, in order, up to `cap` of them.
fn capped_product(parts: Vec<Vec<Value>>, cap: usize) -> Vec<Vec<Value>> {
	let mut acc: Vec<Vec<Value>> = vec![Vec::new()];
	for choices in parts {
		let mut next = Vec::new();
		'fill: for prefix in &acc {
			for choice in &choices {
				if next.len() == cap {
					break 'fill;
				}
				let mut combined = prefix.clone();
				combined.push(choice.clone());
				next.push(combined);
			}
		}
		acc = next;
	}
	acc
}
