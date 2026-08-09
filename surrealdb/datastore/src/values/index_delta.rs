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
/// A document that was both added and removed nets to nothing and is dropped at
/// [`FullTextDelta::normalise`], which keeps the two sets disjoint so a reader
/// can apply them in either order.
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
	fn normalise(&mut self) {
		// `buffer_term_change` keeps a frame's two sets disjoint, so the common
		// path has nothing to drop and must not pay for an intersection.
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

/// Drops every save-point frame and empties the transaction's own, keeping the
/// existing allocations for the next transaction to reuse.
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
	counts: Mutex<HashMap<BufferedIndex, (i64, Uuid)>>,
	/// Indexes that asked for compaction, and the node id to tag the entry with.
	compactions: Mutex<HashMap<BufferedIndex, Uuid>>,
	/// Document ids this transaction moved into or out of each full-text term,
	/// and the summed document-length and count contribution per index.
	///
	/// Both are stacks whose top frame receives new mutations, so a save point
	/// can be rolled back without the buffer keeping contributions whose KV
	/// writes were undone. There is always at least the transaction's own frame.
	term_changes: Mutex<Vec<HashMap<BufferedTerm, FullTextDelta>>>,
	doc_stats: Mutex<Vec<HashMap<BufferedIndex, (DocLengthAndCount, Uuid)>>>,
}

impl Default for IndexDeltaBuffer {
	fn default() -> Self {
		Self {
			counts: Mutex::default(),
			compactions: Mutex::default(),
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
		let mut counts = self.counts.lock();
		counts.entry(index).or_insert((0, nid)).0 += delta;
	}

	/// The net delta buffered for one index so far, so a read in this
	/// transaction can see this transaction's own uncommitted mutations.
	pub fn pending_count(&self, index: &BufferedIndex) -> i64 {
		self.counts.lock().get(index).map(|(delta, _)| *delta).unwrap_or(0)
	}

	/// Record that an index wants compaction once this transaction commits.
	pub fn buffer_compaction_trigger(&self, index: BufferedIndex, nid: Uuid) {
		self.compactions.lock().insert(index, nid);
	}

	/// Record that `doc_id` gained (`add`) or lost a full-text term.
	///
	/// Re-recording the same document in the opposite direction supersedes the
	/// first: a document removed and then re-indexed within one transaction ends
	/// up only in `added`, which is the net truth the compactor needs.
	/// Returns whether this change is the first in its direction for the term,
	/// which is exactly when it adds a key to what the flush will write: the
	/// flush emits one key per (term, direction) whose set is non-empty. The
	/// caller charges the write-cardinality guard on that, so the guard counts
	/// the keys this buffer will write, and counts each of them once.
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
		self.counts
			.lock()
			.drain()
			.filter(|(_, (delta, _))| *delta != 0)
			.map(|(index, (delta, nid))| (index, delta, nid))
			.collect()
	}

	/// Drain the accumulated compaction triggers.
	pub fn take_compactions(&self) -> Vec<(BufferedIndex, Uuid)> {
		self.compactions.lock().drain().collect()
	}

	/// True when nothing is buffered, so commit can skip the flush entirely.
	pub fn is_empty(&self) -> bool {
		self.counts.lock().is_empty()
			&& self.compactions.lock().is_empty()
			&& self.term_changes.lock().iter().all(HashMap::is_empty)
			&& self.doc_stats.lock().iter().all(HashMap::is_empty)
	}

	/// Discard everything buffered. Used when the transaction is cancelled.
	pub fn clear(&self) {
		self.counts.lock().clear();
		self.compactions.lock().clear();
		reset_frames(&mut self.term_changes.lock());
		reset_frames(&mut self.doc_stats.lock());
	}

	/// Open a frame for a save point, so its mutations can be discarded whole.
	///
	/// Both stacks are pushed and popped together by every save-point operation,
	/// so they always stand at the same depth.
	pub fn push_save_point(&self) {
		self.term_changes.lock().push(HashMap::new());
		self.doc_stats.lock().push(HashMap::new());
	}

	/// Fold the save point's frame into the one beneath it: its writes survived,
	/// so its buffered contributions belong to the enclosing scope.
	pub fn release_save_point(&self) {
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
