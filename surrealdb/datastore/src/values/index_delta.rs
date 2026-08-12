//! Per-transaction aggregation of index deltas and compaction triggers.
//!
//! A `COUNT` index is a delta log rather than a counter: a counted mutation
//! appends a signed `!iu` entry instead of updating one shared key, because
//! blind writes of distinct keys never contend whereas a shared key would put
//! every concurrent transaction touching the index into write-write conflict.
//! The same reasoning applies to the `/!ic` compaction queue, and to the
//! full-text `!tt` term changes and `!dc` document statistics.
//!
//! That property is worth keeping, but it does not require one key *per
//! document*. Two transactions never share a key here — the entry is tagged with
//! a per-transaction id — so all of a transaction's mutations to one index can
//! collapse into a single entry carrying the net delta without reintroducing any
//! contention. `CREATE |item:1..=20000|` then writes one `!iu` entry and one
//! `/!ic` entry instead of twenty thousand of each.
//!
//! For a full-text index the same argument bounds the delta count by the
//! **vocabulary** rather than by the work. `!tt` spends a key per (term,
//! document) pair, so a statement indexing 1000 records of 90 distinct terms each
//! writes 90,000 entries where one per distinct term would do. The batched forms
//! are `!tx`, carrying a document-id bitmap per term and direction, and `!dx`,
//! carrying one summed statistic. The per-document families are still read and
//! drained, so an index holding either shape resolves to the same document set.
//!
//! This buffer holds those aggregates until commit. It mirrors
//! [`crate::values::changefeed::Changefeed`]: accumulate during the
//! transaction, flush inside the committing transaction (so the count stays
//! atomic with the document change), discard on cancel.
//!
//! Reads within the same transaction must still observe their own writes, so
//! the count read path adds [`IndexDeltaBuffer::pending_count`] to the entries
//! it scans, and the full-text paths add
//! [`IndexDeltaBuffer::pending_term_change`] and
//! [`IndexDeltaBuffer::pending_doc_stats`].
//!
//! ## Save-point scoping
//!
//! Every family is a stack of frames rather than one map, because what the buffer
//! holds is a description of writes this transaction has made — so a rollback that
//! undoes those writes has to reach the description too. `INSERT` is the case that
//! forces it: it takes a save point per record and rolls it back on a unique
//! conflict or under `INSERT IGNORE`, having already maintained the indexes. A
//! contribution that outlived that rollback would claim a count the table does not
//! hold, or a document carrying a term with no posting behind it.
//!
//! The frames mirror the storage save-point stack: [`IndexDeltaBuffer::push_save_point`]
//! opens one, [`IndexDeltaBuffer::release_save_point`] folds it into its parent,
//! and [`IndexDeltaBuffer::rollback_save_point`] discards it. The bottom frame is
//! the transaction's own and is never popped — the storage layer has no save point
//! matching it.

use std::collections::HashMap;

use parking_lot::Mutex;
use roaring::RoaringTreemap;
use surrealdb_catalog::{DatabaseId, IndexId, NamespaceId};
use surrealdb_expr::val::TableName;
use uuid::Uuid;

use crate::values::fulltext::DocLengthAndCount;

/// Identifies one index within the transaction's buffers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BufferedIndex {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: TableName,
	pub ix: IndexId,
}

/// One full-text term within one index.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BufferedTerm {
	pub index: BufferedIndex,
	pub term: String,
}

/// The document ids one transaction added to, and removed from, one term.
///
/// The two sets are disjoint: a document appears in whichever direction was
/// recorded for it last, so a reader can apply them in either order. That is
/// upheld by the only two things that write a delta — [`Self::buffer_term_change`]
/// clears the opposite set for the document it records, and [`Self::apply`]
/// preserves disjointness when both sides already hold it.
///
/// [`Self::buffer_term_change`]: IndexDeltaBuffer::buffer_term_change
#[derive(Default)]
pub struct FullTextDelta {
	pub added: RoaringTreemap,
	pub removed: RoaringTreemap,
	pub nid: Uuid,
}

impl FullTextDelta {
	/// Folds `other` in, with its direction winning per document: a document the
	/// inner scope added supersedes an outer removal and the other way round.
	fn apply(&mut self, other: &Self) {
		self.removed -= &other.added;
		self.added |= &other.added;
		self.added -= &other.removed;
		self.removed |= &other.removed;
		if !other.nid.is_nil() {
			self.nid = other.nid;
		}
	}

	/// Drops the documents present in both sets, which cancelled out.
	///
	/// A guard on the disjointness the two writers uphold, rather than the thing
	/// that establishes it: on every path they cover there is nothing to drop, and
	/// the early exit is what keeps that path from paying for an intersection.
	fn normalise(&mut self) {
		if self.added.is_disjoint(&self.removed) {
			return;
		}
		let both: RoaringTreemap = &self.added & &self.removed;
		self.added -= &both;
		self.removed -= &both;
	}

	fn is_empty(&self) -> bool {
		self.added.is_empty() && self.removed.is_empty()
	}
}

/// Drops every save-point frame and empties the transaction's own, leaving the
/// stack at the floor depth every other operation assumes.
fn reset_frames<K, V>(frames: &mut Vec<HashMap<K, V>>) {
	frames.truncate(1);
	match frames.first_mut() {
		Some(frame) => frame.clear(),
		None => frames.push(HashMap::new()),
	}
}

/// Folds one document-statistics contribution into a frame's entry for an index.
///
/// The fields sum, so folding is addition; the node id is carried so the flushed
/// key can be tagged with whichever node produced the contribution.
fn fold_doc_stats(
	frame: &mut HashMap<BufferedIndex, (DocLengthAndCount, Uuid)>,
	index: BufferedIndex,
	stats: DocLengthAndCount,
	nid: Uuid,
) {
	let entry = frame.entry(index).or_insert((DocLengthAndCount::default(), nid));
	entry.0.total_docs_length += stats.total_docs_length;
	entry.0.doc_count += stats.doc_count;
	entry.1 = nid;
}

/// Pops a save-point frame off `frames`, or `None` when only the transaction's
/// own frame remains.
///
/// The transaction frame is never popped: it is the buffer's floor, and the
/// storage layer has no save point matching it.
fn pop_save_point<K, V>(frames: &mut Vec<HashMap<K, V>>) -> Option<HashMap<K, V>> {
	(frames.len() > 1).then(|| frames.pop()).flatten()
}

/// Per-transaction buffer of count deltas and compaction triggers.
pub struct IndexDeltaBuffer {
	/// Net signed count delta per count index, with the node id to tag the
	/// flushed entry with.
	counts: Mutex<Vec<HashMap<BufferedIndex, (i64, Uuid)>>>,
	/// Indexes that asked for compaction, and the node id to tag the entry with.
	compactions: Mutex<Vec<HashMap<BufferedIndex, Uuid>>>,
	/// Document ids this transaction moved into or out of each full-text term,
	/// and the summed document-length and count contribution per index.
	term_changes: Mutex<Vec<HashMap<BufferedTerm, FullTextDelta>>>,
	doc_stats: Mutex<Vec<HashMap<BufferedIndex, (DocLengthAndCount, Uuid)>>>,
}

impl Default for IndexDeltaBuffer {
	fn default() -> Self {
		Self {
			counts: Mutex::new(vec![HashMap::new()]),
			compactions: Mutex::new(vec![HashMap::new()]),
			term_changes: Mutex::new(vec![HashMap::new()]),
			doc_stats: Mutex::new(vec![HashMap::new()]),
		}
	}
}

impl IndexDeltaBuffer {
	pub fn new() -> Self {
		Self::default()
	}

	/// Add `delta` to the running total for one count index.
	pub fn buffer_count_delta(&self, index: BufferedIndex, delta: i64, nid: Uuid) {
		if delta == 0 {
			return;
		}
		let mut frames = self.counts.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		frame.entry(index).or_insert((0, nid)).0 += delta;
	}

	/// The net delta buffered for one index so far, so a read in this
	/// transaction can see this transaction's own uncommitted mutations.
	///
	/// Summed across the open save points, because their writes are visible to
	/// this transaction until one is rolled back.
	pub fn pending_count(&self, index: &BufferedIndex) -> i64 {
		self.counts
			.lock()
			.iter()
			.filter_map(|frame| frame.get(index).map(|(delta, _)| *delta))
			.sum()
	}

	/// Record that an index wants compaction once this transaction commits.
	pub fn buffer_compaction_trigger(&self, index: BufferedIndex, nid: Uuid) {
		let mut frames = self.compactions.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		frame.insert(index, nid);
	}

	/// Record that `doc_id` gained (`add`) or lost a full-text term.
	///
	/// Re-recording the same document in the opposite direction supersedes the
	/// first: a document removed and then re-indexed within one transaction ends
	/// up only in `added`, which is the net truth the compactor needs.
	/// Returns whether this change is the first in its direction for the term
	/// *within the frame it lands in*, which is what the caller charges the
	/// write-cardinality guard on: the flush emits one key per (term, direction)
	/// whose set is non-empty, so a first change is a key the flush will write.
	///
	/// Per frame rather than per term, so a term first touched inside a save point
	/// that already has it in an enclosing scope is charged twice, and a charge for
	/// a frame that is later rolled back is never returned. Both over-count, which
	/// is the direction a limit has to err in, and both follow the guard's standing
	/// rule that a reservation is not refunded.
	#[must_use]
	pub fn buffer_term_change(
		&self,
		term: BufferedTerm,
		doc_id: u64,
		add: bool,
		nid: Uuid,
	) -> bool {
		let mut frames = self.term_changes.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		let delta = frame.entry(term).or_default();
		delta.nid = nid;
		// A direction emptied again by a later cancellation keeps the charge it
		// already took, over-counting by one in that case — the safe direction
		// for a limit.
		if add {
			let first = delta.added.is_empty();
			delta.removed.remove(doc_id);
			delta.added.insert(doc_id);
			first
		} else {
			let first = delta.removed.is_empty();
			delta.added.remove(doc_id);
			delta.removed.insert(doc_id);
			first
		}
	}

	/// This transaction's own contribution to one term, so a query in the same
	/// transaction observes the documents it has just indexed.
	///
	/// Returns the added and removed sets; both are empty when nothing is
	/// buffered for the term.
	pub fn pending_term_change(&self, term: &BufferedTerm) -> (RoaringTreemap, RoaringTreemap) {
		let mut merged = FullTextDelta::default();
		for frame in self.term_changes.lock().iter() {
			if let Some(delta) = frame.get(term) {
				merged.apply(delta);
			}
		}
		merged.normalise();
		(merged.added, merged.removed)
	}

	/// Add one document's length to a full-text index's running statistics.
	pub fn buffer_doc_stats(&self, index: BufferedIndex, stats: DocLengthAndCount, nid: Uuid) {
		let mut frames = self.doc_stats.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		fold_doc_stats(frame, index, stats, nid);
	}

	/// This transaction's own document-statistics contribution, so a scorer in
	/// the same transaction weighs the documents it has just indexed.
	pub fn pending_doc_stats(&self, index: &BufferedIndex) -> DocLengthAndCount {
		let mut total = DocLengthAndCount::default();
		for frame in self.doc_stats.lock().iter() {
			if let Some((stats, _)) = frame.get(index) {
				total.total_docs_length += stats.total_docs_length;
				total.doc_count += stats.doc_count;
			}
		}
		total
	}

	/// Drain the accumulated term changes, dropping any that cancelled out.
	pub fn take_term_changes(&self) -> Vec<(BufferedTerm, FullTextDelta)> {
		let mut frames = self.term_changes.lock();
		let mut merged: HashMap<BufferedTerm, FullTextDelta> = HashMap::new();
		for frame in frames.drain(..) {
			for (term, delta) in frame {
				merged.entry(term).or_default().apply(&delta);
			}
		}
		frames.push(HashMap::new());
		merged
			.into_iter()
			.filter_map(|(term, mut delta)| {
				delta.normalise();
				(!delta.is_empty()).then_some((term, delta))
			})
			.collect()
	}

	/// Drain the accumulated document statistics.
	///
	/// An index whose documents cancelled out contributes nothing, so writing an
	/// entry would only add work for the next compaction.
	pub fn take_doc_stats(&self) -> Vec<(BufferedIndex, DocLengthAndCount, Uuid)> {
		let mut frames = self.doc_stats.lock();
		let mut merged: HashMap<BufferedIndex, (DocLengthAndCount, Uuid)> = HashMap::new();
		for frame in frames.drain(..) {
			for (index, (stats, nid)) in frame {
				fold_doc_stats(&mut merged, index, stats, nid);
			}
		}
		frames.push(HashMap::new());
		merged
			.into_iter()
			.filter(|(_, (stats, _))| stats.doc_count != 0 || stats.total_docs_length != 0)
			.map(|(index, (stats, nid))| (index, stats, nid))
			.collect()
	}

	/// Drain the accumulated count deltas.
	///
	/// Indexes whose mutations cancelled out (a record created and deleted in
	/// the same transaction) net to zero and are dropped: there is no delta to
	/// record, so writing an entry would only add work for the next compaction.
	pub fn take_counts(&self) -> Vec<(BufferedIndex, i64, Uuid)> {
		let mut frames = self.counts.lock();
		let mut merged: HashMap<BufferedIndex, (i64, Uuid)> = HashMap::new();
		for frame in frames.drain(..) {
			for (index, (delta, nid)) in frame {
				let entry = merged.entry(index).or_insert((0, nid));
				entry.0 += delta;
				entry.1 = nid;
			}
		}
		frames.push(HashMap::new());
		merged
			.into_iter()
			.filter(|(_, (delta, _))| *delta != 0)
			.map(|(index, (delta, nid))| (index, delta, nid))
			.collect()
	}

	/// Drain the accumulated compaction triggers.
	pub fn take_compactions(&self) -> Vec<(BufferedIndex, Uuid)> {
		let mut frames = self.compactions.lock();
		let mut merged: HashMap<BufferedIndex, Uuid> = HashMap::new();
		for frame in frames.drain(..) {
			merged.extend(frame);
		}
		frames.push(HashMap::new());
		merged.into_iter().collect()
	}

	/// True when nothing is buffered, so commit can skip the flush entirely.
	pub fn is_empty(&self) -> bool {
		self.counts.lock().iter().all(HashMap::is_empty)
			&& self.compactions.lock().iter().all(HashMap::is_empty)
			&& self.term_changes.lock().iter().all(HashMap::is_empty)
			&& self.doc_stats.lock().iter().all(HashMap::is_empty)
	}

	/// Discard everything buffered. Used when the transaction is cancelled.
	pub fn clear(&self) {
		reset_frames(&mut self.counts.lock());
		reset_frames(&mut self.compactions.lock());
		reset_frames(&mut self.term_changes.lock());
		reset_frames(&mut self.doc_stats.lock());
	}

	/// Open a frame for a save point, so its mutations can be discarded whole.
	///
	/// Every stack is pushed and popped together by every save-point operation, so
	/// they always stand at the same depth.
	pub fn push_save_point(&self) {
		self.counts.lock().push(HashMap::new());
		self.compactions.lock().push(HashMap::new());
		self.term_changes.lock().push(HashMap::new());
		self.doc_stats.lock().push(HashMap::new());
	}

	/// Fold the save point's frame into the one beneath it: its writes survived,
	/// so its buffered contributions belong to the enclosing scope.
	pub fn release_save_point(&self) {
		{
			let mut frames = self.counts.lock();
			if let Some(top) = pop_save_point(&mut frames) {
				let parent = frames.last_mut().expect("popping left at least one frame");
				for (index, (delta, nid)) in top {
					let entry = parent.entry(index).or_insert((0, nid));
					entry.0 += delta;
					entry.1 = nid;
				}
			}
		}
		{
			let mut frames = self.compactions.lock();
			if let Some(top) = pop_save_point(&mut frames) {
				let parent = frames.last_mut().expect("popping left at least one frame");
				parent.extend(top);
			}
		}
		{
			let mut frames = self.term_changes.lock();
			if let Some(top) = pop_save_point(&mut frames) {
				let parent = frames.last_mut().expect("popping left at least one frame");
				for (term, delta) in top {
					parent.entry(term).or_default().apply(&delta);
				}
			}
		}
		let mut frames = self.doc_stats.lock();
		if let Some(top) = pop_save_point(&mut frames) {
			let parent = frames.last_mut().expect("popping left at least one frame");
			for (index, (stats, nid)) in top {
				fold_doc_stats(parent, index, stats, nid);
			}
		}
	}

	/// Discard the save point's frame: its KV writes were rolled back, so its
	/// buffered contributions describe writes that no longer exist.
	pub fn rollback_save_point(&self) {
		pop_save_point(&mut self.counts.lock());
		pop_save_point(&mut self.compactions.lock());
		pop_save_point(&mut self.term_changes.lock());
		pop_save_point(&mut self.doc_stats.lock());
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn term() -> BufferedTerm {
		BufferedTerm {
			index: BufferedIndex {
				ns: NamespaceId(1),
				db: DatabaseId(2),
				tb: "t".into(),
				ix: IndexId(3),
			},
			term: "hello".to_owned(),
		}
	}

	fn stats(len: i128, count: i64) -> DocLengthAndCount {
		DocLengthAndCount {
			total_docs_length: len,
			doc_count: count,
		}
	}

	/// A rolled-back save point's KV writes are undone, so its buffered
	/// description of them must not survive: flushing it would claim a document
	/// carries a term whose posting no longer exists.
	#[test]
	fn a_rolled_back_save_point_keeps_none_of_its_deltas() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();

		buffer.push_save_point();
		let _ = buffer.buffer_term_change(term(), 7, true, nid);
		buffer.buffer_doc_stats(term().index, stats(100, 1), nid);
		buffer.rollback_save_point();

		assert_eq!(buffer.pending_term_change(&term()), Default::default());
		assert_eq!(buffer.pending_doc_stats(&term().index), stats(0, 0));
		assert!(buffer.take_term_changes().is_empty());
		assert!(buffer.take_doc_stats().is_empty());
	}

	/// The count families are scoped by the same frames. `INSERT ... ON DUPLICATE
	/// KEY UPDATE` on a counted table is the shape that needs it: the abandoned
	/// create attempt counts a row the table does not hold, and a contribution
	/// surviving its rollback overstates the count for good, since a count index is
	/// a delta log with nothing to correct it later.
	#[test]
	fn a_rolled_back_save_point_keeps_none_of_its_counts() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();
		let index = term().index;

		buffer.push_save_point();
		buffer.buffer_count_delta(index.clone(), 1, nid);
		buffer.buffer_compaction_trigger(index.clone(), nid);
		buffer.rollback_save_point();

		assert_eq!(buffer.pending_count(&index), 0, "the abandoned attempt must not be counted");
		assert!(buffer.take_counts().is_empty());
		assert!(buffer.take_compactions().is_empty(), "nothing was written to compact");
	}

	/// A released save point's counts belong to the enclosing scope, and a read in
	/// the transaction sees them from the moment they are buffered — the writes
	/// they describe are visible until something rolls them back.
	///
	/// Nested, because a single scope cannot tell framing apart from one flat map:
	/// releasing into a parent sums either way. What distinguishes them is an inner
	/// rollback under an outer scope that survives — the inner contribution has to
	/// go and the outer one has to stay, which one map cannot do.
	#[test]
	fn nested_save_points_keep_only_the_counts_that_survived() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();
		let index = term().index;

		buffer.buffer_count_delta(index.clone(), 2, nid);
		buffer.push_save_point();
		buffer.buffer_count_delta(index.clone(), 3, nid);
		// Asked for inside the scope that survives, so the release has to carry it up.
		buffer.buffer_compaction_trigger(index.clone(), nid);
		assert_eq!(buffer.pending_count(&index), 5, "an open scope's writes are visible");

		buffer.push_save_point();
		buffer.buffer_count_delta(index.clone(), 10, nid);
		assert_eq!(buffer.pending_count(&index), 15);
		buffer.rollback_save_point();
		assert_eq!(buffer.pending_count(&index), 5, "the inner scope's writes went with it");

		buffer.release_save_point();
		assert_eq!(buffer.pending_count(&index), 5, "the outer scope's survived");
		assert_eq!(buffer.take_counts(), vec![(index.clone(), 5, nid)]);
		assert_eq!(
			buffer.take_compactions(),
			vec![(index, nid)],
			"a released scope's compaction wake-up has to reach the flush, or the deltas it \
			 folded sit uncompacted until something else asks"
		);
	}

	/// A released save point's writes survived, so its deltas belong to the
	/// enclosing scope and must be flushed with it.
	#[test]
	fn a_released_save_point_hands_its_deltas_up() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();

		buffer.push_save_point();
		let _ = buffer.buffer_term_change(term(), 7, true, nid);
		buffer.buffer_doc_stats(term().index, stats(100, 1), nid);
		buffer.release_save_point();

		let (added, removed) = buffer.pending_term_change(&term());
		assert!(added.contains(7));
		assert!(removed.is_empty());
		assert_eq!(buffer.pending_doc_stats(&term().index), stats(100, 1));

		let flushed = buffer.take_term_changes();
		assert_eq!(flushed.len(), 1);
		assert!(flushed[0].1.added.contains(7));
		assert_eq!(buffer.take_doc_stats().len(), 1);
	}

	/// The retry shape `INSERT ... ON DUPLICATE KEY UPDATE` produces: a create
	/// attempt is rolled back, then an update indexes the record for real. Only
	/// the surviving attempt may contribute.
	#[test]
	fn a_rollback_then_retry_contributes_only_the_retry() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();

		// The abandoned create attempt indexed a term the final record lacks.
		buffer.push_save_point();
		let _ = buffer.buffer_term_change(term(), 7, true, nid);
		buffer.buffer_doc_stats(term().index, stats(100, 1), nid);
		buffer.rollback_save_point();

		// The retry indexes the record it actually stored.
		let _ = buffer.buffer_term_change(term(), 9, true, nid);
		buffer.buffer_doc_stats(term().index, stats(40, 1), nid);

		let flushed = buffer.take_term_changes();
		assert_eq!(flushed.len(), 1);
		assert!(!flushed[0].1.added.contains(7), "the abandoned attempt must not contribute");
		assert!(flushed[0].1.added.contains(9));
		assert_eq!(buffer.take_doc_stats()[0].1, stats(40, 1));
	}

	/// A document removed and re-indexed within one transaction is still
	/// present, so the net contribution is one addition and no statistics move.
	#[test]
	fn a_remove_then_reindex_nets_to_an_addition_and_no_statistics() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();

		let _ = buffer.buffer_term_change(term(), 7, false, nid);
		buffer.buffer_doc_stats(term().index, stats(-100, -1), nid);
		let _ = buffer.buffer_term_change(term(), 7, true, nid);
		buffer.buffer_doc_stats(term().index, stats(100, 1), nid);

		let flushed = buffer.take_term_changes();
		assert_eq!(flushed.len(), 1);
		assert!(flushed[0].1.added.contains(7));
		assert!(flushed[0].1.removed.is_empty(), "the removal was superseded");
		assert!(buffer.take_doc_stats().is_empty(), "unchanged statistics need no entry");
	}
}
