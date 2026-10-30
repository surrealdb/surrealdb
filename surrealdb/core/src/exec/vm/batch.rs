//! Bounded batches of records for the reads of storage that fetch records by
//! id: an index, a full-text query, a fused bitmap, a nearest-neighbor
//! search.
//!
//! A batch of records is decoded whole before its first row runs, and every
//! operator above a read holds whole batches, so a batch of wide records
//! (embeddings, large documents) is as large as the records are wide times its
//! row count. A read therefore sizes its batches by bytes as well as by rows:
//! the first batch is a small probe, since no row size is known yet, and each
//! later one is sized from the batch before it so that a batch like it stays
//! near the `scan_batch_bytes` budget of the execution config. A batch is
//! never smaller than one row and never larger than its row cap, and it at
//! most doubles from one batch to the next, so wide records that follow a run
//! of narrow ones are not fetched in a batch the narrow ones sized.
//!
//! The size of a record is that of the record as the host reads it. Fields a
//! program computes for a row exist only inside the program and are not
//! counted: a program runs a batch's rows one at a time.
//!
//! A reader of stored records ([`RecordReader::stored`]) reads each chunk of
//! records as their stored bytes, a fraction of their decoded size, sizes the
//! next chunk from those bytes, and decodes the chunk a batch at a time: a
//! batch holds at most the byte budget plus one record of decoded values,
//! however much wider its records are than the ones the chunk was sized from.
//! It may instead hand each chunk on undecoded ([`RecordReader::next_stored`])
//! to a caller that runs its rows on the stored bytes, which holds no decoded
//! values for them. Such a reader decodes only the top-level fields its
//! [`DecodeFields`] lists where the plan established that nothing reads the
//! rest, and never puts a record in the transaction's record cache.

use std::collections::VecDeque;

use super::services::{EngineServices, Services};
use crate::exec::operators::scan::common::{
	DecodeFields, RECORD_PROBE_ROWS, ReadAhead, ReadAheadRecord, approx_value_size, next_batch_len,
	probe_batch_len,
};
use crate::exec::permission::PhysicalPermission;
use crate::exec::{ExecutionContext, FlowResult};
use crate::kvs::CachePolicy;
use crate::val::{RecordId, Value};

/// The number of records the next batch of a read may take.
pub(super) struct BatchSizer {
	budget: usize,
	max_rows: usize,
	len: usize,
}

impl BatchSizer {
	/// A sizer for a read under `ctx`, bounded by its `scan_batch_bytes` and
	/// by `max_rows` rows per batch.
	pub(super) fn new(ctx: &ExecutionContext, max_rows: usize) -> Self {
		Self::with_budget(ctx.root().ctx.config.exec.scan_batch_bytes, max_rows)
	}

	/// A sizer bounded by `budget` bytes and `max_rows` rows per batch.
	pub(super) fn with_budget(budget: usize, max_rows: usize) -> Self {
		let max_rows = max_rows.max(1);
		Self {
			budget,
			max_rows,
			len: probe_batch_len(budget, RECORD_PROBE_ROWS.min(max_rows)),
		}
	}

	/// The number of records the next batch takes.
	pub(super) fn len(&self) -> usize {
		self.len
	}

	/// Size the next batch from `rows`, the records the last batch read. A
	/// batch that read no record says nothing about their size.
	pub(super) fn observe(&mut self, rows: &[Value]) {
		let bytes = rows.iter().map(approx_value_size).sum();
		self.observe_bytes(bytes, rows.len());
	}

	/// Size the next batch from a read that found `rows` records in `bytes`.
	fn observe_bytes(&mut self, bytes: usize, rows: usize) {
		self.len = next_batch_len(bytes, rows, self.len, self.budget, self.max_rows);
	}
}

/// Read `rids` as one batch of `sizer`, and size its next batch from what
/// came back.
pub(super) async fn read_sized(
	ctx: &ExecutionContext,
	sizer: &mut BatchSizer,
	rids: &[RecordId],
	version: Option<u64>,
	policy: CachePolicy,
) -> FlowResult<Vec<Value>> {
	#[cfg(test)]
	BATCHES.with(|batches| batches.borrow_mut().push(rids.len()));
	let rows = EngineServices::new(ctx).read_records(rids, version, policy).await?;
	sizer.observe(&rows);
	Ok(rows)
}

/// A queue of record ids read in batches sized to the byte budget.
pub(super) struct RecordReader {
	pending: VecDeque<RecordId>,
	sizer: BatchSizer,
	version: Option<u64>,
	policy: CachePolicy,
	/// The records read as stored bytes and not yet decoded, for a reader of
	/// stored records.
	ahead: Option<ReadAhead>,
}

impl RecordReader {
	/// A reader of records as of `version`, under `policy`, with batches of at
	/// most `max_rows` records.
	pub(super) fn new(
		ctx: &ExecutionContext,
		max_rows: usize,
		version: Option<u64>,
		policy: CachePolicy,
	) -> Self {
		Self {
			pending: VecDeque::new(),
			sizer: BatchSizer::new(ctx, max_rows),
			version,
			policy,
			ahead: None,
		}
	}

	/// A reader of records as of `version` that reads at most `max_rows`
	/// records at once as their stored bytes and decodes them a byte budget
	/// at a time: only the fields `decode` lists when it is given, whole
	/// records otherwise. The program applies the table's permission to the
	/// rows, so the reader checks none.
	pub(super) fn stored(
		ctx: &ExecutionContext,
		max_rows: usize,
		version: Option<u64>,
		decode: Option<DecodeFields>,
	) -> Self {
		let budget = ctx.root().ctx.config.exec.scan_batch_bytes;
		Self {
			pending: VecDeque::new(),
			sizer: BatchSizer::with_budget(budget, max_rows),
			version,
			policy: CachePolicy::ReadOnly,
			ahead: Some(ReadAhead::new(PhysicalPermission::Allow, false, decode, budget)),
		}
	}

	/// The number of records the next read takes.
	pub(super) fn read_len(&self) -> usize {
		self.sizer.len()
	}

	/// Queue `rids` behind those already queued.
	pub(super) fn push(&mut self, rids: Vec<RecordId>) {
		self.pending.extend(rids);
	}

	/// Whether every queued id has been read and decoded.
	pub(super) fn is_drained(&self) -> bool {
		self.pending.is_empty() && self.ahead.as_ref().is_none_or(ReadAhead::is_empty)
	}

	/// Read the next batch of queued ids: the records that exist, in queue
	/// order. Empty when none of the batch exists, or the queue is drained.
	pub(super) async fn next_rows(&mut self, ctx: &ExecutionContext) -> FlowResult<Vec<Value>> {
		if let Some(ahead) = &mut self.ahead
			&& !ahead.is_empty()
		{
			return decode_batch(ahead, ctx).await;
		}
		let take = self.sizer.len().min(self.pending.len());
		if take == 0 {
			return Ok(Vec::new());
		}
		let batch: Vec<RecordId> = self.pending.drain(..take).collect();
		let Some(ahead) = &mut self.ahead else {
			return read_sized(ctx, &mut self.sizer, &batch, self.version, self.policy).await;
		};
		let (bytes, rows) =
			EngineServices::new(ctx).read_stored(ahead, batch, self.version).await?;
		self.sizer.observe_bytes(bytes, rows);
		decode_batch(ahead, ctx).await
	}

	/// For a reader of stored records, read the next batch of queued ids and
	/// hand on the records that exist undecoded, in queue order
	/// ([`ReadAhead::take_records`]): the caller runs its rows on their stored
	/// bytes. The batch is sized from the stored bytes of the batch before it,
	/// and so holds about the byte budget in stored bytes. Empty when none of
	/// the batch exists, or the queue is drained.
	pub(super) async fn next_stored(
		&mut self,
		ctx: &ExecutionContext,
	) -> FlowResult<VecDeque<(RecordId, ReadAheadRecord)>> {
		let Some(ahead) = &mut self.ahead else {
			return Err(
				anyhow::anyhow!("a reader of decoded records was asked for stored ones").into()
			);
		};
		if ahead.is_empty() {
			let take = self.sizer.len().min(self.pending.len());
			if take == 0 {
				return Ok(VecDeque::new());
			}
			let batch: Vec<RecordId> = self.pending.drain(..take).collect();
			#[cfg(test)]
			BATCHES.with(|batches| batches.borrow_mut().push(batch.len()));
			let (bytes, rows) =
				EngineServices::new(ctx).read_stored(ahead, batch, self.version).await?;
			self.sizer.observe_bytes(bytes, rows);
		}
		Ok(ahead.take_records())
	}
}

/// Decode the next batch of the records `ahead` holds.
async fn decode_batch(ahead: &mut ReadAhead, ctx: &ExecutionContext) -> FlowResult<Vec<Value>> {
	let rows = ahead.decode_batch(ctx).await?;
	#[cfg(test)]
	BATCHES.with(|batches| batches.borrow_mut().push(rows.len()));
	Ok(rows)
}

#[cfg(test)]
thread_local! {
	/// The size of each batch of records a read on this thread took, in
	/// order, for tests that must know how a read was batched: the record ids
	/// a batch asked for or, for a reader of stored records, the records a
	/// batch decoded.
	pub(super) static BATCHES: std::cell::RefCell<Vec<usize>> =
		const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
mod tests {
	use super::*;

	const MAX_ROWS: usize = 1000;

	fn wide(floats: usize) -> Value {
		Value::from(vec![Value::from(0.5f64); floats])
	}

	#[test]
	fn the_first_batch_is_a_probe_that_follows_the_budget() {
		assert_eq!(BatchSizer::with_budget(2 << 20, MAX_ROWS).len(), RECORD_PROBE_ROWS);
		assert_eq!(BatchSizer::with_budget(256 << 10, MAX_ROWS).len(), 4);
		assert_eq!(BatchSizer::with_budget(1, MAX_ROWS).len(), 1);
		assert_eq!(BatchSizer::with_budget(2 << 20, 8).len(), 8, "the row cap bounds the probe");
	}

	#[test]
	fn narrow_rows_double_the_batch_up_to_the_row_cap() {
		let mut sizer = BatchSizer::with_budget(2 << 20, MAX_ROWS);
		let mut lens = vec![sizer.len()];
		for _ in 0..8 {
			sizer.observe(&vec![Value::from("narrow"); sizer.len()]);
			lens.push(sizer.len());
		}
		assert_eq!(lens[..4], [32, 64, 128, 256]);
		assert_eq!(*lens.last().unwrap(), MAX_ROWS);
	}

	#[test]
	fn wide_rows_shrink_the_batch_to_the_budget() {
		let mut sizer = BatchSizer::with_budget(2 << 20, MAX_ROWS);
		let rows = vec![wide(4096); sizer.len()];
		let per_row = approx_value_size(&rows[0]);
		sizer.observe(&rows);
		assert_eq!(sizer.len(), (2 << 20) / per_row);
		assert!(sizer.len() < RECORD_PROBE_ROWS);
	}

	#[test]
	fn wide_rows_after_narrow_ones_are_not_fetched_in_the_narrow_batch() {
		let mut sizer = BatchSizer::with_budget(2 << 20, MAX_ROWS);
		sizer.observe(&vec![Value::from("narrow"); sizer.len()]);
		let before = sizer.len();
		assert_eq!(before, 64);
		sizer.observe(&vec![wide(4096); before]);
		assert!(sizer.len() < 32, "the wide batch shrinks the next one: {}", sizer.len());
	}

	#[test]
	fn a_row_wider_than_the_budget_still_takes_one_row() {
		let mut sizer = BatchSizer::with_budget(1, MAX_ROWS);
		sizer.observe(&[wide(1024)]);
		assert_eq!(sizer.len(), 1);
	}

	#[test]
	fn a_batch_that_read_nothing_keeps_the_size() {
		let mut sizer = BatchSizer::with_budget(2 << 20, MAX_ROWS);
		let before = sizer.len();
		sizer.observe(&[]);
		assert_eq!(sizer.len(), before);
	}
}
