//! The COUNT baseline policy: how a COUNT build merges the queued old states of the records its
//! scan passed into the baseline it indexes, across checkpointed spans
//! ([`super::kind::BaselinePolicy::MergeQueuedSpans`]).
//!
//! Every other kind baselines from the scan alone; the build reaches this code only through the
//! baseline hook.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

use anyhow::Result;

use super::builder::Building;
use super::kind::BaselinePolicy;
use super::replay::*;
use super::{Appending, BuildGeneration, PrimaryAppendingTicket};
use crate::ctx::FrozenContext;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::idx::PreviousBuildPrimaryKey;
use crate::key::{KVKey, KVValue, Resumable};
use crate::kvs::{DatastoreError, Direction, INDEXING_BATCH_SIZE, Transaction};
use crate::val::{RecordId, RecordIdKey, RecordIdentity};

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

	/// Whether a marker is set aside for a record of a later span.
	pub(super) fn holds_early(&self) -> bool {
		!self.early.is_empty()
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

pub(super) struct CountPrimaryAppendingScan<'a> {
	pub(super) lookup_tx: &'a Transaction,
	pub(super) progress: &'a mut CountPrimaryProgress,
	pub(super) through: Option<&'a RecordIdKey>,
	pub(super) live_ids: &'a BTreeSet<RecordIdentity>,
	/// The keys of the older nodes' markers the batch's scan read, by record.
	pub(super) older_markers: &'a [(RecordIdentity, PreviousBuildPrimaryKey)],
	pub(super) initial_count: usize,
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
pub(super) fn queued_no_later(a: PrimaryAppendingTicket, b: PrimaryAppendingTicket) -> bool {
	(a.ticket, a.mutation_seq) <= (b.ticket, b.mutation_seq)
}

/// The predicate results a record baselined from its queued mutation's old
/// state carries: no state before it, and the old state's own result.
pub(super) fn baselined_match(count_cond_match: Option<(bool, bool)>) -> Option<(bool, bool)> {
	count_cond_match.map(|(old_matches, _)| (false, old_matches))
}

/// A `!bp` marker whose queued mutation has confirmed the marker's record,
/// with that mutation.
pub(super) struct Marker {
	pub(super) ptr: PrimaryAppendingTicket,
	pub(super) appending: Appending,
}

impl Marker {
	/// The earlier of two markers for one record.
	pub(super) fn earlier(self, other: Self) -> Self {
		if queued_no_later(self.ptr, other.ptr) {
			self
		} else {
			other
		}
	}
}

/// When the merge meets a marker, against the span that holds its record's
/// own key.
pub(super) enum Met {
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
		let mut rc = false;
		let live_ids = BTreeSet::new();
		let exec = self.exec_ctx(ctx).await?;
		let az_fn = StreamingAnalyzerFunction::new(&exec);
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
				&az_fn,
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
	pub(super) async fn index_missing_count_primary_appendings(
		&self,
		ctx: &FrozenContext,
		scan: CountPrimaryAppendingScan<'_>,
		az_fn: &StreamingAnalyzerFunction<'_>,
		rc: &mut bool,
	) -> Result<usize> {
		if self.kind().baseline() != BaselinePolicy::MergeQueuedSpans {
			return Ok(0);
		}
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
			self.index_queued_baseline(ctx, az_fn, rc, appending).await?;
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
				self.index_queued_baseline(ctx, az_fn, rc, appending).await?;
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
	pub(super) async fn queued_appending(
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

	/// The marker an older node left for `record` under its spelling, with the
	/// key it sits at.
	///
	/// Only an id holding a number is spelled differently there, and the key can
	/// be another record's, so the marker counts only once its queued mutation
	/// confirms the record.
	pub(super) async fn older_marker(
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
	pub(super) async fn settled_in_span(
		&self,
		scan: &CountPrimaryAppendingScan<'_>,
		generation: BuildGeneration,
		record: &RecordIdentity,
	) -> Result<bool> {
		Ok(scan.live_ids.contains(record)
			|| self.own_marker(scan.lookup_tx, generation, &record.0).await?.is_some())
	}

	/// Indexes the old state a record's queued mutation carries, as the
	/// record's baseline.
	pub(super) async fn index_queued_baseline(
		&self,
		ctx: &FrozenContext,
		az_fn: &StreamingAnalyzerFunction<'_>,
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
			az_fn,
			&None,
			InitialIndexValue {
				rid: &rid,
				opt_values: appending.old_values,
				count_cond_match,
				doc_id: None,
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
}
