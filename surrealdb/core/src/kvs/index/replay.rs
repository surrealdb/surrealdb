use std::borrow::Cow;
use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::Result;
use chrono::Utc;
use common::time::sleep;
use uuid::Uuid;
use web_time::Instant;

use super::builder::Building;
use super::count::{
	CountPrimaryAppendingScan, CountPrimaryProgress, Marker, baselined_match, queued_no_later,
};
use super::kind::{BatchRange, InitialBatchSink, SinkFrame};
use super::{
	Appending, BUILD_CLOSING_SLEEP, BuildGeneration, ExistingPrimaryAppending, IndexBuildPhase,
	IndexBuildReportStatus, LEGACY_BATCH_ID, PrimaryAppendingTicket,
};
use crate::catalog::Record;
use crate::catalog::providers::NodeProvider;
use crate::ctx::FrozenContext;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::idx::PreviousBuildPrimaryKey;
use crate::idx::docids::TableDocIds;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::index::{IndexOperation, recorded_shapes};
use crate::key::schema::{
	BuildAppendKey, BuildReservationKey, DocPendingKey, IndexAppendKey, RecordKey, RecordPrefix,
};
use crate::key::{KVKeyDecode, KVSubspace, KVValue, Key, Resumable};
use crate::kvs::{
	DatastoreError, Direction, INDEXING_BATCH_SIZE, LayoutSet, Transaction, Val,
	is_retryable_transaction_conflict,
};
use crate::val::{RecordId, RecordIdKey, RecordIdentity, Value};

/// The image of the record `rid` that an index entry is evaluated over, from
/// the `data` its stored value decodes to.
///
/// The write path evaluates an entry over an image that carries the record's
/// own `id`, and a column or a guard that reads a component of it
/// ([`PhysicalIndexEntry::reads_record_id`](crate::exec::permission::PhysicalIndexEntry::reads_record_id))
/// reads it there. A stored value may carry none, or another, so when `needed`
/// the image is `data` with `id` set to `rid`, the key the record is stored
/// under. Otherwise, and for data that is no object, it is `data` as it is.
pub(super) fn entry_image<'a>(data: &'a Value, rid: &RecordId, needed: bool) -> Cow<'a, Value> {
	let Value::Object(object) = data else {
		return Cow::Borrowed(data);
	};
	if !needed || matches!(object.get("id"), Some(Value::RecordId(id)) if id == rid) {
		return Cow::Borrowed(data);
	}
	let mut object = object.clone();
	object.insert("id", Value::RecordId(rid.clone()));
	Cow::Owned(Value::Object(object))
}

/// Maximum consecutive commit-conflict retries for one page of the rebuild of a
/// generation's primary-append markers before the build gives up.
///
/// Unlike the reclaim sweep this cannot leave its work for a later build: the
/// initial scan about to run is the reader the markers are for, and a page
/// left unwritten would have it baseline records as though they had no queued
/// mutation. Giving up therefore fails the build, which a rebuild recovers from
/// by starting a fresh generation.
const PRIMARY_APPEND_REBUILD_MAX_RETRIES: usize = 10;

/// What indexing one initial scan batch did.
pub(super) struct InitialBatch<'s> {
	/// The records it indexed.
	pub(super) indexed: usize,
	/// How many of the batch's records, from its first, it processed: all of them, unless the
	/// kind's sink cut the batch where one frame would hold too many vectors. The rest belong
	/// to the next transaction. At least 1 for a batch that was not aborted.
	pub(super) consumed: usize,
	/// The sink frame the batch was applied in, until the build transaction ends.
	pub(super) frame: Option<SinkFrame<'s>>,
}

pub(super) struct InitialIndexValue<'a> {
	pub(super) rid: &'a RecordId,
	pub(super) opt_values: Option<Vec<Value>>,
	pub(super) count_cond_match: Option<(bool, bool)>,
	/// The record's doc-ID in the table's space, when the caller resolved it.
	pub(super) doc_id: Option<crate::idx::docids::DocId>,
}

/// When a replay pass over the queued mutations may return.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReplayUntil {
	/// Return once a pass has applied every mutation that one read of the
	/// queues found, which takes a read returning less than a full batch from
	/// each queue.
	///
	/// This is the pass that runs while admissions are open. Writers keep
	/// queueing behind it, so the queues are not expected to be empty at any
	/// read: under a steady stream of commits a pass that waited for that could
	/// run for as long as the stream does. What a caught-up pass leaves is what
	/// writers committed while it applied its last batch, and the `Closing`
	/// passes that follow drain it with admissions fenced.
	CaughtUp,
	/// Return only once a read finds both queues empty.
	///
	/// Bounded only while admissions are fenced, which makes the queued work
	/// finite: the passes after `Closing` is durable.
	Drained,
}

impl Building {
	/// Replay both legacy and durable queued mutations until `until` holds.
	///
	/// Legacy `!ig`/`!ip` appendings can exist from already committed work
	/// created by older paths. Durable `!bg` appendings are scoped to the
	/// current generation and must be replayed before the build can move online.
	pub(super) async fn index_appending_loop(
		&self,
		until: ReplayUntil,
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
			#[cfg(test)]
			if until == ReplayUntil::CaughtUp {
				catch_up_hook::before_read(self.ctx.node_id()).await;
			}

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
			let full_batch = INDEXING_BATCH_SIZE as usize;
			let caught_up = keys.len() < full_batch && durable_keys.len() < full_batch;
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
			self.spend_batch()?;
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
				match self.commit_build_tx(&tx).await {
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
					Ok(()) => match self.commit_build_tx(&tx).await {
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
			// Only reached once this round's batches have committed: a retried
			// batch `continue`s and reads the queues again.
			if until == ReplayUntil::CaughtUp && caught_up {
				break;
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
			self.spend_batch()?;
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
			if let Err(err) = self.commit_build_tx(&tx).await {
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
			if let Err(err) = self.commit_build_tx(&tx).await {
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
	///
	/// A record with no queued mutation goes to `sink` when the kind's sink
	/// takes records, and through the kind's write path otherwise. A sink
	/// takes records in scan order until one more would exceed what one of its
	/// frames inserts; the batch is cut before that record, and the records
	/// from it on — queued or not — are left to the next transaction.
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn index_initial_batch<'s>(
		&self,
		ctx: &FrozenContext,
		tx: &Transaction,
		values: &[(Vec<u8>, Val)],
		initial_count: usize,
		v1_appending_sentinel: &mut bool,
		count_progress: &mut Option<CountPrimaryProgress>,
		sink: &'s InitialBatchSink,
	) -> Result<InitialBatch<'s>> {
		let mut rc = false;
		let mut count = 0;
		let mut live_ids = BTreeSet::new();
		let mut last_live_id = None;
		let mut older_markers = Vec::new();
		let generation = self.build_generation.load(Ordering::Acquire);
		// Each record's key, decoded once for the scan and the older markers.
		let keys =
			values.iter().map(|(k, _)| RecordKey::decode_key(k)).collect::<Result<Vec<_>>>()?;
		let range = match (values.first(), values.last()) {
			(Some((first, _)), Some((last, _))) => Some(BatchRange {
				first,
				last,
			}),
			_ => None,
		};
		let batch = self.batch_context(ctx, range).await?;
		let mut sink_records = sink.records(self.ctx.node_id());
		let mut consumed = values.len();
		let lookup_tx = self.new_read_tx().await?;
		// The index's columns and `COUNT` guard, as the engine
		// evaluates them: compiled once for the build, and the same
		// expressions the write path evaluates for the same definition, so a
		// record indexed by this scan and one indexed by a concurrent write
		// are judged alike.
		let exec = self.exec_ctx(ctx).await?;
		let entry = self.compiled_index(&exec).await?;
		// The analyzer's `FUNCTION` clause, compiled once for the batch rather
		// than once per record, in the same frame the columns are evaluated in.
		let az_fn = StreamingAnalyzerFunction::new(&exec);
		// The index holds the values a read of the record sees, so a record
		// stamped before a migration step of the table is brought through the
		// steps it has not passed.
		let replay = crate::exec::migrate::replay_for(&exec, self.ikb.table(), None)
			.await
			.map_err(crate::exec::migrate::into_error)?;
		let result = async {
			// Whether any record of the batch can have a queued mutation, read
			// once on the snapshot every per-record lookup below reads: with no
			// marker of the generation or legacy marker of the index, each
			// lookup would miss.
			let markers_held = self.holds_primary_markers(&lookup_tx, generation).await?;
			// A record is baselined from its first queued mutation, which an
			// older node may have marked under its own spelling.
			let mut older_held = if markers_held && generation != 0 {
				self.older_markers_held(&lookup_tx, generation, values, &keys).await?
			} else {
				Vec::new()
			};
			// With no queued mutation to baseline from, and none set aside for
			// the merge to look up among the batch's records, an unguarded COUNT
			// index counts each record without reading what it holds.
			let unread = !markers_held
				&& !count_progress.as_ref().is_some_and(CountPrimaryProgress::holds_early)
				&& self.kind().indexes_records_unread();
			// The generations of the batch's records, checked as each is read.
			let mut layouts = LayoutSet::new(self.ix_key.ns, self.ix_key.db, None);
			// Index the records.
			for (i, ((_, v), key)) in values.iter().zip(keys).enumerate() {
				// Per-record analysis (deserialization, tokenization, filters)
				// is synchronous CPU work with no other guaranteed await; yield
				// so a long batch cannot starve the runtime worker.
				yield_now!();
				if self.is_aborted().await {
					return Ok((count, i, true));
				}
				self.is_beyond_threshold(Some(initial_count + count))?;
				let rid: Arc<RecordId> = RecordId {
					table: key.tb.into_owned(),
					key: key.id.into_owned(),
				}
				.into();
				if unread {
					// The merge looks a record up among the live ones only for a
					// marker, and this batch's snapshot holds none, so only the
					// span's end is kept.
					if count_progress.is_some() {
						last_live_id = Some(rid.key.clone());
					}
					self.index_initial_values(
						ctx,
						&az_fn,
						&batch.fulltext,
						InitialIndexValue {
							rid: rid.as_ref(),
							opt_values: Some(Vec::new()),
							count_cond_match: None,
							doc_id: None,
						},
						&mut rc,
					)
					.await?;
					count += 1;
					continue;
				}
				// The record as it was written: an id its encoding left out is
				// the key's. Its generation is admitted against the table's
				// layouts first. Descriptors are immutable, and one is retired
				// only by a floor move whose precondition is that no row names
				// its generation, so `lookup_tx`, opened after the batch was
				// read, lists every generation the batch names. A packed record is
				// decoded with the layouts that admitted it.
				let (stamp, admitted) =
					layouts.admit_with_layouts(&lookup_tx, &rid.table, v.as_slice()).await?;
				let mut val = Record::decode_admitted_as_written(
					v.as_slice(),
					admitted.map(|held| &**held),
					|| (*rid).clone(),
				)?;
				if let Some(replay) = &replay {
					replay
						.apply(&exec, &mut val.data, stamp)
						.await
						.map_err(crate::exec::migrate::into_error)?;
				}
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
				let existing = if markers_held {
					self.check_existing_primary_appending(
						&lookup_tx,
						&rid.key,
						older,
						v1_appending_sentinel,
					)
					.await?
				} else {
					None
				};
				let (opt_values, count_cond_match, queued) = if let Some(a) = existing {
					(a.old_values, baselined_match(a.count_cond_match), true)
				} else {
					// Otherwise, proceed with normal indexing.
					let image = entry_image(&val.data, &rid, entry.reads_record_id());
					let opt_values = entry.values(&image, &exec).await?;
					// COUNT WHERE indexes have no indexed values, so the
					// initial scan carries the predicate result separately.
					// The scan is the record's entry into the index, so the
					// before-image it is compared against is its absence.
					let count_cond_match =
						entry.guard_matches(&image, &exec).await?.map(|matches| (false, matches));
					(opt_values, count_cond_match, false)
				};
				// A record with no queued mutation goes to the kind's sink when it
				// takes records. A record with one keeps the write path, which
				// queues it behind the mutation replay applies on top.
				if !queued && sink.takes_records() {
					// The sink records no value shapes: a value it holds no entry for, or
					// flattens, is recorded here, in the build transaction that takes it.
					let absent = opt_values
						.as_ref()
						.filter(|values| recorded_shapes(values, &self.ix).next().is_some())
						.cloned();
					if !sink.take(&mut sink_records, &rid.key, opt_values)? {
						consumed = i;
						break;
					}
					if absent.is_some() {
						IndexOperation::new(
							ctx,
							self.ix_key.ns,
							self.ix_key.db,
							self.tb,
							&self.ix,
							None,
							absent,
							rid.as_ref(),
						)
						.within_build()
						.record_value_shapes()
						.await?;
					}
					count += 1;
					continue;
				}
				self.index_initial_values(
					ctx,
					&az_fn,
					&batch.fulltext,
					InitialIndexValue {
						rid: rid.as_ref(),
						opt_values,
						count_cond_match,
						doc_id: None,
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
						&az_fn,
						&mut rc,
					)
					.await?;
			}
			// Trigger compaction if needed.
			self.check_index_compaction(tx, &mut rc).await?;
			// We're done.
			Ok((count, consumed, false))
		}
		.await;
		let cancel_result = lookup_tx.cancel().await;
		let (indexed, consumed, aborted) = match result {
			Ok(done) => {
				cancel_result?;
				done
			}
			Err(err) => {
				let _ = cancel_result;
				return Err(err);
			}
		};
		// The records the sink took, applied in the build transaction over the
		// range the batch consumed. An aborted batch is discarded whole.
		let frame = match (aborted, consumed) {
			(false, 1..) => {
				let range = BatchRange {
					first: &values[0].0,
					last: &values[consumed - 1].0,
				};
				sink.apply(ctx, &self.aborted, range, sink_records).await?
			}
			_ => None,
		};
		Ok(InitialBatch {
			indexed,
			consumed,
			frame,
		})
	}

	/// Whether `tx` holds any primary-appending marker a record of this build
	/// could be looked up by: a `!bp` marker of `generation`, in any spelling,
	/// or a legacy `!ip` marker of the index.
	pub(super) async fn holds_primary_markers(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
	) -> Result<bool> {
		if generation != 0
			&& !tx.keys(self.ikb.new_bp_range(generation)?, 1, 0, None).await?.is_empty()
		{
			return Ok(true);
		}
		Ok(!tx.keys_raw(self.ikb.new_ip_raw_range()?, 1, 0, None).await?.is_empty())
	}

	/// Index one initial-scan value.
	pub(super) async fn index_initial_values(
		&self,
		ctx: &FrozenContext,
		az_fn: &StreamingAnalyzerFunction<'_>,
		fulltext_index: &Option<FullTextIndex>,
		value: InitialIndexValue<'_>,
		rc: &mut bool,
	) -> Result<()> {
		let InitialIndexValue {
			rid,
			opt_values,
			count_cond_match,
			doc_id,
		} = value;
		let mut io = IndexOperation::new(
			ctx,
			self.ix_key.ns,
			self.ix_key.db,
			self.tb,
			&self.ix,
			None,
			opt_values,
			rid,
		)
		.within_build();
		if let Some((old_matches, new_matches)) = count_cond_match {
			io = io.with_count_cond_match(old_matches, new_matches);
		}
		if let Some(doc_id) = doc_id {
			io = io.with_doc_id(doc_id);
		}
		if let Some(fulltext_index) = fulltext_index {
			io.compute_fulltext_with_index(az_fn, fulltext_index, rc).await
		} else {
			io.compute(az_fn, rc).await
		}
	}

	/// The marker at `record`'s own key, with the queued mutation it points at,
	/// when that mutation is `record`'s.
	///
	/// A marker an older node wrote for another record can sit there — the older
	/// spelling of `[1, NONE]` is `[1]`'s key under this release — so the marker
	/// at the key counts only once its queued mutation confirms the record.
	pub(super) async fn own_marker(
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
	pub(super) async fn confirmed_marker(
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
		az_fn: &StreamingAnalyzerFunction<'_>,
		fulltext_index: &Option<FullTextIndex>,
		appending: Appending,
		rc: &mut bool,
	) -> Result<RecordIdKey> {
		let rid_key = appending.id;
		// A queued delete (old present, new absent) on a doc-ID index is a consumer
		// of the shared doc-ID space: the delete path deferred the `!dj`/`!dd`
		// removal precisely so this replay could still resolve the doc-ID (full-text
		// reads it; HNSW/DiskAnn capture it into the pending). The mapping is
		// table-scoped, so it may only be reclaimed by the LAST building consumer —
		// reclaiming it while a sibling doc-ID index is still building would leave
		// that sibling unable to resolve the doc-ID (guarded by
		// `other_doc_id_index_building` below).
		let reclaim_doc_id = appending.new_values.is_none()
			&& appending.old_values.is_some()
			&& self.kind().defers_doc_ids();
		let rid = RecordId {
			table: self.ikb.table().clone(),
			key: rid_key.clone(),
		};
		let mut io = IndexOperation::new(
			ctx,
			self.ix_key.ns,
			self.ix_key.db,
			self.tb,
			&self.ix,
			appending.old_values,
			appending.new_values,
			&rid,
		)
		.within_build();
		if let Some((old_matches, new_matches)) = appending.count_cond_match {
			io = io.with_count_cond_match(old_matches, new_matches);
		}
		if let Some(fulltext_index) = fulltext_index {
			io.compute_fulltext_with_index(az_fn, fulltext_index, rc).await?;
		} else {
			io.compute(az_fn, rc).await?;
		}
		if reclaim_doc_id {
			let tx = ctx.tx();
			if self.other_doc_id_index_building(ctx).await? {
				// A sibling doc-ID index is still building and may still need the
				// shared `!dj`/`!dd` mapping to replay its own copy of this delete,
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

	/// Builds the datastore key of one record on the indexed table.
	pub(super) fn record_key<'a>(&'a self, id: &'a RecordIdKey) -> RecordKey<'a> {
		RecordKey {
			ns: self.ix_key.ns,
			db: self.ix_key.db,
			tb: std::borrow::Cow::Borrowed(self.ikb.table()),
			id: std::borrow::Cow::Borrowed(id),
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
		let batch = self.batch_context(ctx, None).await?;
		let exec = self.exec_ctx(ctx).await?;
		let az_fn = StreamingAnalyzerFunction::new(&exec);
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
				let rid_key =
					self.apply_appending(ctx, &az_fn, &batch.fulltext, appending, &mut rc).await?;
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
		let batch = self.batch_context(ctx, None).await?;
		let exec = self.exec_ctx(ctx).await?;
		let az_fn = StreamingAnalyzerFunction::new(&exec);
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
				let rid_key =
					self.apply_appending(ctx, &az_fn, &batch.fulltext, appending, &mut rc).await?;
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

	pub(super) async fn check_index_compaction(
		&self,
		tx: &Transaction,
		rc: &mut bool,
	) -> Result<()> {
		if !*rc {
			return Ok(());
		}
		IndexOperation::compaction_trigger(&self.ikb, tx, self.ctx.node_id()).await?;
		*rc = false;
		Ok(())
	}
}

#[cfg(test)]
mod entry_image_tests {
	use super::*;
	use crate::val::{Array, Object};

	fn record(n: i64) -> RecordId {
		RecordId {
			table: "t".into(),
			key: RecordIdKey::Array(Array(vec![Value::from("f"), Value::from(n)])),
		}
	}

	fn stored(fields: &[(&str, Value)]) -> Value {
		Value::Object(Object::from_iter(
			fields.iter().map(|(name, value)| ((*name).to_owned(), value.clone())),
		))
	}

	#[test]
	fn an_entry_that_does_not_read_the_id_is_evaluated_over_the_stored_value() {
		let data = stored(&[("v", Value::from(7))]);
		let image = entry_image(&data, &record(2), false);
		assert!(matches!(image, Cow::Borrowed(_)));
		assert_eq!(*image, data);
	}

	#[test]
	fn a_stored_value_with_no_id_is_given_the_key_it_is_stored_under() {
		let data = stored(&[("v", Value::from(7))]);
		let image = entry_image(&data, &record(2), true);
		assert!(matches!(image, Cow::Owned(_)));
		assert_eq!(*image, stored(&[("v", Value::from(7)), ("id", Value::RecordId(record(2)))]));
		// The stored value is left as it was read.
		assert_eq!(data, stored(&[("v", Value::from(7))]));
	}

	#[test]
	fn a_stored_id_that_is_the_key_is_kept() {
		let data = stored(&[("id", Value::RecordId(record(2))), ("v", Value::from(7))]);
		let image = entry_image(&data, &record(2), true);
		assert!(matches!(image, Cow::Borrowed(_)));
		assert_eq!(*image, data);
	}

	#[test]
	fn a_stored_id_that_is_not_the_key_is_replaced_by_it() {
		let data = stored(&[("id", Value::RecordId(record(9))), ("v", Value::from(7))]);
		let image = entry_image(&data, &record(2), true);
		assert_eq!(*image, stored(&[("id", Value::RecordId(record(2))), ("v", Value::from(7))]));
	}

	#[test]
	fn a_stored_value_that_is_no_object_is_evaluated_as_it_is() {
		for data in [Value::None, Value::from(7), Value::from("f")] {
			let image = entry_image(&data, &record(2), true);
			assert!(matches!(image, Cow::Borrowed(_)));
			assert_eq!(*image, data);
		}
	}
}

/// A test hook run before every read of a [`ReplayUntil::CaughtUp`] pass, so a
/// test can commit a write between any two of the pass's reads and hold the
/// queues non-empty at each of them.
#[cfg(test)]
pub(super) mod catch_up_hook {
	use std::collections::HashMap;
	use std::sync::{LazyLock, Mutex};

	use tokio::sync::{mpsc, oneshot};
	use uuid::Uuid;

	/// One request per read: the pass waits until the receiver answers it.
	pub(in super::super) type Request = oneshot::Sender<()>;

	static HOOKS: LazyLock<Mutex<HashMap<Uuid, mpsc::UnboundedSender<Request>>>> =
		LazyLock::new(Default::default);

	/// Unregisters the node's hook when dropped.
	pub(in super::super) struct Guard(Uuid);

	impl Drop for Guard {
		fn drop(&mut self) {
			HOOKS.lock().expect("catch-up hooks lock").remove(&self.0);
		}
	}

	/// Routes the reads of every catch-up pass on `node` to the returned
	/// receiver until the guard is dropped.
	pub(in super::super) fn register(node: Uuid) -> (Guard, mpsc::UnboundedReceiver<Request>) {
		let (tx, rx) = mpsc::unbounded_channel();
		HOOKS.lock().expect("catch-up hooks lock").insert(node, tx);
		(Guard(node), rx)
	}

	pub(super) async fn before_read(node: Uuid) {
		let hook = HOOKS.lock().expect("catch-up hooks lock").get(&node).cloned();
		let Some(hook) = hook else {
			return;
		};
		let (done, wait) = oneshot::channel();
		if hook.send(done).is_ok() {
			// A dropped receiver releases the pass as an answer would.
			let _ = wait.await;
		}
	}
}
