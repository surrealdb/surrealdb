//! The initial scan of an index build, indexed by several tasks at once.
//!
//! The scan reads the table in key order, one batch at a time, and hands each
//! batch to a task of its own, which indexes it in a transaction of its own
//! and commits it. The reader runs ahead of the tasks by at most a round of
//! batches. Once every batch of a round has committed, the continuation
//! checkpoint moves to the last record of the round, in a transaction that also
//! renews the build's ownership. The checkpoint therefore never covers a
//! record whose batch has not committed, and a scan resumed from it indexes
//! again the records of the batches past it that had committed, which a kind
//! scanned this way ([`Kind::scans_in_parallel`]) leaves unchanged.
//!
//! A batch transaction writes no build state. It reads `!bs` registered for
//! commit-time conflict detection and checks that this builder still owns the
//! generation it builds: a takeover, a phase change or the abort of the build
//! that commits `!bs` after the batch's snapshot fails the batch's commit, so
//! no batch commits for a generation this builder no longer owns. The
//! checkpoint writes `!bs` only once no batch is in flight.
//!
//! [`Kind::scans_in_parallel`]: super::kind::Kind::scans_in_parallel

use std::sync::Arc;

use anyhow::Result;
use surrealdb_strand::Strand;

use super::builder::Building;
use super::kind::InitialBatchSink;
use super::replay::{InitialBatch, InitialIndexValue, entry_image};
use super::{BuildGeneration, IndexBuildPhase};
use crate::catalog::{Record, TableLayouts};
use crate::ctx::FrozenContext;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::object_extract::stored_record_object_in;
use crate::idx::config::BuildParallelism;
use crate::idx::docids::TableDocIds;
use crate::key::schema::RecordKey;
use crate::key::{AnyRange, KVKeyDecode, Resumable};
use crate::kvs::{
	DatastoreError, Direction, INDEXING_BATCH_MAX_BYTES, INDEXING_SCAN_BATCH_SIZE, LayoutSet,
	Transaction, Val,
};
use crate::val::{RecordId, RecordIdKey, Value};

/// The most tasks the scan indexes batches on when its parallelism is `auto`.
const AUTO_SCAN_TASKS: usize = 16;

/// One record of a batch, with the values its index entries hold.
struct PreparedRecord {
	rid: Arc<RecordId>,
	opt_values: Option<Vec<Value>>,
	count_cond_match: Option<(bool, bool)>,
}

/// What reading a batch's records for its index values found.
enum Prepared {
	/// Every record, with its values.
	Records(Vec<PreparedRecord>),
	/// A record of the batch may have a queued mutation.
	Queued,
	/// The build was aborted.
	Aborted,
}

/// How the indexing of one batch ended.
enum BatchOutcome {
	/// The batch committed, having indexed this many records.
	Indexed(usize),
	/// The build was aborted, and the batch committed nothing.
	Aborted,
}

/// The values at `paths` in the stored record `bytes`, whose generation
/// `layouts` admitted, read in place: `None` unless the record is a document
/// that holds every path through objects alone, which is where the index's
/// columns read the same values off the decoded record.
fn fields_in_place(
	bytes: &[u8],
	layouts: Option<&TableLayouts>,
	paths: &[&[Strand]],
) -> Option<Vec<Value>> {
	// No column reads nothing, so the record need not even be parsed.
	if paths.is_empty() {
		return Some(Vec::new());
	}
	let data = stored_record_object_in(bytes, layouts)??;
	let mut values = Vec::with_capacity(paths.len());
	for path in paths {
		let (last, steps) = path.split_last()?;
		let mut object = data;
		for step in steps {
			object = object.get(step.as_str()).ok()??.as_ordered_object().ok()??;
		}
		values.push(object.get(last.as_str()).ok()??.decode().ok()?);
	}
	Some(values)
}

impl Building {
	/// How many tasks this build's initial scan indexes batches on: one for a
	/// kind whose batches must commit in order.
	pub(super) fn initial_scan_tasks(&self) -> usize {
		if !self.kind().scans_in_parallel() {
			return 1;
		}
		match crate::idx::env::IndexEnv::config(&*self.ctx).index_build_parallelism {
			// A test build scans serially unless its configuration asks otherwise, so the
			// tests of the per-batch checkpoint protocol see one batch committed at a time.
			BuildParallelism::Auto if cfg!(test) => 1,
			BuildParallelism::Auto => BuildParallelism::Auto.tasks().min(AUTO_SCAN_TASKS),
			parallelism @ (BuildParallelism::Serial | BuildParallelism::Fixed(_)) => {
				parallelism.tasks()
			}
		}
	}

	/// Indexes the records of `range` on `tasks` tasks, from the record after
	/// `initial_count` records already indexed, and adds the records it
	/// indexes to `initial_count`.
	///
	/// Answers `false` when the build was aborted, which leaves the checkpoint
	/// at the last round every batch of which committed.
	pub(super) async fn scan_in_parallel<R>(
		self: &Arc<Self>,
		generation: BuildGeneration,
		range: R,
		initial_count: &mut usize,
		last_prepare_remove_check: &mut web_time::Instant,
		tasks: usize,
		mut batch_size: u32,
	) -> Result<bool>
	where
		R: AnyRange + Resumable + Clone,
	{
		let mut next = Some(range);
		while next.is_some() {
			let mut round = Vec::with_capacity(tasks);
			let mut last_key: Option<Vec<u8>> = None;
			// Read one batch per task, each handed to its task as soon as it is
			// read, so the tasks index while the reader reads on. An abort, or an
			// error reading, ends the round early: its tasks are joined below
			// before the scan returns, so none outlives it.
			let mut read_failure = None;
			while round.len() < tasks {
				let Some(rng) = next.take() else {
					break;
				};
				if self.is_aborted().await {
					break;
				}
				if let Err(err) = self.is_beyond_threshold(None) {
					read_failure = Some(err);
					break;
				}
				let read = async {
					let tx = self.new_read_tx().await?;
					let read = async {
						self.check_prepare_remove_with_tx(last_prepare_remove_check, &tx).await?;
						tx.batch_keys_vals_raw(rng.clone(), batch_size, None).await
					}
					.await;
					tx.cancel().await?;
					read
				}
				.await;
				let batch = match read {
					Ok(batch) => batch,
					Err(err) => {
						read_failure = Some(err);
						break;
					}
				};
				next = batch
					.next
					.and(batch.result.last())
					.map(|(last, _)| rng.clone().resume_after(last, Direction::Forward));
				let Some((last, _)) = batch.result.last() else {
					break;
				};
				if let Err(err) = self.spend_batch() {
					read_failure = Some(err);
					break;
				}
				let bytes: usize = batch.result.iter().map(|(k, v)| k.len() + v.len()).sum();
				let avg = (bytes / batch.result.len()).max(1);
				batch_size = (INDEXING_BATCH_MAX_BYTES / avg)
					.clamp(1, INDEXING_SCAN_BATCH_SIZE as usize) as u32;
				last_key = Some(last.clone());
				let building = Arc::clone(self);
				let base = *initial_count;
				round.push(common::rt::spawn(async move {
					building.index_scan_batch(generation, batch.result, base).await
				}));
			}
			// Every batch of the round has ended before the checkpoint moves, and
			// before an error is reported, so no batch commits after it.
			let mut indexed = 0;
			let mut aborted = false;
			let mut task_failure = None;
			for task in round {
				match task.await {
					Ok(Ok(BatchOutcome::Indexed(n))) => indexed += n,
					Ok(Ok(BatchOutcome::Aborted)) => aborted = true,
					Ok(Err(err)) => {
						self.abort();
						task_failure.get_or_insert(err);
					}
					Err(err) => {
						self.abort();
						task_failure.get_or_insert(anyhow::anyhow!(
							"an index build scan task failed: {err}"
						));
					}
				}
			}
			if let Some(err) = task_failure {
				return Err(read_failure.unwrap_or(err));
			}
			if aborted || self.is_aborted().await {
				return Ok(false);
			}
			// The batches the round started were read in key order and all
			// committed, so the checkpoint moves past them even when the round
			// ended early: a pass that spent its budget, or met the memory
			// threshold, hands the build back with them kept, and the next owner
			// resumes after them instead of wiping the index and rescanning.
			if let Some(last_key) = &last_key {
				let cursor = RecordKey::decode_key(last_key)?.id.into_owned();
				if !self.checkpoint_round(generation, &cursor, *initial_count + indexed).await? {
					return Ok(false);
				}
				*initial_count += indexed;
			}
			if let Some(err) = read_failure {
				return Err(err);
			}
			if last_key.is_none() {
				break;
			}
		}
		Ok(!self.is_aborted().await)
	}

	/// Indexes `values`, one batch of the scan, in a transaction of its own,
	/// retrying it on a retryable conflict.
	///
	/// The batch's records are decoded and their index values evaluated first,
	/// on a read snapshot taken after the batch was read, and the write
	/// transaction opens only then, to assign doc-IDs, write the entries and
	/// commit: a commit is checked for conflicts against every commit made
	/// since its transaction began, so the shorter the transaction, the fewer
	/// batches of the scan it is checked against. A batch holding a record with
	/// a queued mutation is indexed as the serial scan indexes one, whole, in
	/// the write transaction ([`Building::index_initial_batch`]).
	async fn index_scan_batch(
		self: Arc<Self>,
		generation: BuildGeneration,
		values: Vec<(Vec<u8>, Val)>,
		initial_count: usize,
	) -> Result<BatchOutcome> {
		let prepared = match self.prepare_scan_batch(generation, &values, initial_count).await? {
			Prepared::Records(records) => Some(records),
			Prepared::Queued => None,
			Prepared::Aborted => return Ok(BatchOutcome::Aborted),
		};
		let sink = InitialBatchSink::Pending;
		loop {
			if self.is_aborted().await {
				return Ok(BatchOutcome::Aborted);
			}
			let ctx = self.new_write_tx_ctx().await?;
			let tx = ctx.tx();
			let indexed = async {
				self.fence_build_ownership(&tx, generation).await?;
				if let Some(records) = &prepared {
					return self
						.write_scan_batch(&ctx, &tx, records)
						.await
						.map(|n| (n, values.len()));
				}
				let mut v1_appending_sentinel = false;
				let mut count_progress = None;
				let InitialBatch {
					indexed,
					consumed,
					frame,
				} = self
					.index_initial_batch(
						&ctx,
						&tx,
						&values,
						initial_count,
						&mut v1_appending_sentinel,
						&mut count_progress,
						&sink,
					)
					.await?;
				// A batch the sink takes no records of is indexed whole, unless
				// the abort cut it short, which the caller checks next.
				debug_assert!(frame.is_none());
				Ok::<_, anyhow::Error>((indexed, consumed))
			}
			.await;
			let indexed = match indexed {
				Ok((indexed, consumed)) => {
					if consumed < values.len() || self.is_aborted().await {
						tx.cancel().await?;
						return Ok(BatchOutcome::Aborted);
					}
					indexed
				}
				Err(err) => {
					if self.is_aborted().await {
						let _ = tx.cancel().await;
						return Ok(BatchOutcome::Aborted);
					}
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict in a parallel index scan batch, retrying",
						)
						.await
					{
						continue;
					}
					return Err(err);
				}
			};
			// Without a sync of its own: the round's checkpoint syncs once every batch of
			// the round has committed, and makes them durable with it, so a crash never
			// loses a batch the checkpoint covers.
			tx.defer_sync();
			match tx.commit().await {
				Ok(()) => {
					self.note_committed();
					return Ok(BatchOutcome::Indexed(indexed));
				}
				Err(err) => {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict committing a parallel index scan batch, retrying",
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

	/// The records of `values` with the index values each holds, as a read
	/// snapshot taken now sees them, or [`Prepared::Queued`] when any record
	/// of the batch may have a queued mutation, which the scan indexes from
	/// instead.
	async fn prepare_scan_batch(
		&self,
		generation: BuildGeneration,
		values: &[(Vec<u8>, Val)],
		initial_count: usize,
	) -> Result<Prepared> {
		let ctx = self.new_read_tx_ctx().await?;
		let tx = ctx.tx();
		let prepared = async {
			if self.holds_primary_markers(&tx, generation).await? {
				return Ok::<_, anyhow::Error>(Prepared::Queued);
			}
			let exec = self.exec_ctx(&ctx).await?;
			let entry = self.compiled_index(&exec).await?;
			let replay = crate::exec::migrate::replay_for(&exec, self.ikb.table(), None)
				.await
				.map_err(crate::exec::migrate::into_error)?;
			// An index whose columns are plain fields reads them off each stored
			// record in place, rather than decoding the whole record, wherever
			// the record holds every one of them. A table with migration steps
			// to replay is read whole.
			let fields = if replay.is_none() {
				entry.field_paths()
			} else {
				None
			};
			let mut layouts = LayoutSet::new(self.ix_key.ns, self.ix_key.db, None);
			let mut records = Vec::with_capacity(values.len());
			for (k, v) in values {
				yield_now!();
				if self.is_aborted().await {
					return Ok(Prepared::Aborted);
				}
				self.is_beyond_threshold(Some(initial_count + records.len()))?;
				let key = RecordKey::decode_key(k)?;
				let rid: Arc<RecordId> = RecordId {
					table: key.tb.into_owned(),
					key: key.id.into_owned(),
				}
				.into();
				// The record as it was written, its generation admitted against
				// the table's layouts, as the serial scan reads it.
				let (stamp, admitted) =
					layouts.admit_with_layouts(&tx, &rid.table, v.as_slice()).await?;
				if let Some(paths) = &fields
					&& let Some(values) =
						fields_in_place(v.as_slice(), admitted.map(|held| &**held), paths)
				{
					records.push(PreparedRecord {
						rid,
						opt_values: Some(values),
						count_cond_match: None,
					});
					continue;
				}
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
				let image = entry_image(&val.data, &rid, entry.reads_record_id());
				let opt_values = entry.values(&image, &exec).await?;
				let count_cond_match =
					entry.guard_matches(&image, &exec).await?.map(|matches| (false, matches));
				records.push(PreparedRecord {
					rid,
					opt_values,
					count_cond_match,
				});
			}
			Ok(Prepared::Records(records))
		}
		.await;
		let cancelled = tx.cancel().await;
		let prepared = prepared?;
		cancelled?;
		Ok(prepared)
	}

	/// Writes the entries of `records` in `tx`, the transaction of `ctx`, and
	/// answers how many records it indexed.
	async fn write_scan_batch(
		&self,
		ctx: &FrozenContext,
		tx: &Transaction,
		records: &[PreparedRecord],
	) -> Result<usize> {
		let exec = self.exec_ctx(ctx).await?;
		let az_fn = StreamingAnalyzerFunction::new(&exec);
		// The batch's doc-IDs, resolved and assigned in one read and one draw
		// from the table's sequence rather than one of each per record.
		let doc_ids = if self.ix.has_entry_doc_ids() {
			let ids: Vec<RecordIdKey> =
				records.iter().map(|record| record.rid.key.clone()).collect();
			TableDocIds::new(self.ix_key.ns, self.ix_key.db, self.ikb.table().clone())
				.resolve_or_assign_many(&**ctx, &ids)
				.await?
				.into_iter()
				.map(Some)
				.collect()
		} else {
			vec![None; records.len()]
		};
		let mut rc = false;
		for (record, doc_id) in records.iter().zip(doc_ids) {
			self.index_initial_values(
				ctx,
				&az_fn,
				&None,
				InitialIndexValue {
					rid: record.rid.as_ref(),
					opt_values: record.opt_values.clone(),
					count_cond_match: record.count_cond_match,
					doc_id,
				},
				&mut rc,
			)
			.await?;
		}
		self.check_index_compaction(tx, &mut rc).await?;
		Ok(records.len())
	}

	/// Fails unless this builder owns `generation` in its `Building` phase, as
	/// `tx` reads `!bs`, registered for commit-time conflict detection so that
	/// a change of the state committed after `tx`'s snapshot fails its commit.
	async fn fence_build_ownership(
		&self,
		tx: &Transaction,
		generation: BuildGeneration,
	) -> Result<()> {
		let Some(current) = tx.getu_key(&self.ikb.new_bs_key()).await? else {
			return Err(DatastoreError::CorruptedIndex(
				"Index build state is missing during a parallel scan batch",
			)
			.into());
		};
		if current.generation != generation
			|| current.owner != Some(self.owner)
			|| current.phase != IndexBuildPhase::Building
		{
			return Err(DatastoreError::IndexingBuildingCancelled {
				reason: format!("Index build ownership was lost for {}", self.ix.name),
			}
			.into());
		}
		Ok(())
	}

	/// Moves the continuation checkpoint to `cursor`, having indexed
	/// `initial_count` records, and renews this builder's ownership, in one
	/// transaction. Answers `false` when the build was aborted first.
	async fn checkpoint_round(
		&self,
		generation: BuildGeneration,
		cursor: &RecordIdKey,
		initial_count: usize,
	) -> Result<bool> {
		loop {
			if self.is_aborted().await {
				return Ok(false);
			}
			let tx = self.new_write_tx().await?;
			let staged = async {
				self.maintain_build_ownership(&tx, generation, &[IndexBuildPhase::Building])
					.await?;
				self.checkpoint_initial_scan(&tx, generation, cursor, initial_count).await
			}
			.await;
			if let Err(err) = staged {
				if self
					.cancel_and_retryable_conflict(
						&tx,
						&err,
						"transient conflict checkpointing a parallel index scan, retrying",
					)
					.await
				{
					continue;
				}
				return Err(err);
			}
			match self.commit_build_tx(&tx).await {
				Ok(()) => return Ok(true),
				Err(err) => {
					if self
						.cancel_and_retryable_conflict(
							&tx,
							&err,
							"transient conflict checkpointing a parallel index scan, retrying",
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
}
