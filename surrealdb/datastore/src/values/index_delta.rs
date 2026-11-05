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
//! The buffer also names the DiskANN pending-state guard shards
//! (`!dy{shard}`) this transaction wrote pending updates into. Unlike the
//! other families, these are not written into the committing transaction:
//! the commit arms them in a transaction of its own, so the user's
//! transaction never reads or writes a key the compactor writes
//! (`Transaction::commit`). Lookups in this transaction treat a named shard as
//! holding entries ([`IndexDeltaBuffer::pending_guard_shards`]).
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
//! and [`IndexDeltaBuffer::rollback_save_point`] discards it. The stack's contract
//! is [`crate::values::save_point`]; every family here is one of its stacks, and
//! each release folds its frame by whatever combining that family's values means.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;
use roaring::RoaringTreemap;
use surrealdb_catalog::{DatabaseId, IndexId, NamespaceId};
use surrealdb_expr::val::TableName;
use uuid::Uuid;

use crate::pending_guard::GuardedIndex;
use crate::values::fulltext::DocLengthAndCount;
use crate::values::graph::GraphFoldScope;
use crate::values::save_point::{pop_save_point, reset_frames};

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
/// Each document carries a net change, the sum of the additions (+1) and
/// removals (−1) recorded for it, and the two sets encode that sum:
///
/// - in `added` only: +1, the transaction gave the document the term;
/// - in `removed` only: −1, the transaction took the term away;
/// - in both: 0, the transaction removed the term and gave it back, or the other way round.
///
/// Every reader of the flushed `!tx` pair, and of [`IndexDeltaBuffer::pending_term_change`],
/// counts an `added` entry as +1 and a `removed` entry as −1, so a document in
/// both leaves the term's membership as it was. It is still named as changed:
/// a re-index records a change for every term the document carries before and
/// after it, including the terms it keeps, and a reader that needs to know
/// which documents changed since the last compaction takes that from which
/// documents the uncompacted changes name, not from their sum.
///
/// The sum is held to −1..=+1. A document's membership of a term moves by one
/// at a time, so on every path that indexes correctly the sum never leaves that
/// range; the clamp only absorbs a change repeated in one direction.
///
/// [`IndexDeltaBuffer::buffer_term_change`] and [`Self::apply`] are the only
/// writers, and both keep that encoding.
#[derive(Default)]
pub struct FullTextDelta {
	pub added: RoaringTreemap,
	pub removed: RoaringTreemap,
	/// On an index that keeps its postings in blocks, each document's last
	/// posting, `(document, frequency, length)`, in ascending document order,
	/// one entry per document. A frequency of zero: the document's last change
	/// carried no posting (a removal), or one too large to hold, so no posting
	/// of it holds. Empty on any other index.
	pub postings: Vec<(u64, u32, u32)>,
	pub nid: Uuid,
}

impl FullTextDelta {
	/// Records one addition (`add`) or removal of `doc_id`, and returns whether
	/// the set the change lands in was empty before it.
	fn record(&mut self, doc_id: u64, add: bool) -> bool {
		let (this, other) = if add {
			(&mut self.added, &mut self.removed)
		} else {
			(&mut self.removed, &mut self.added)
		};
		let first = this.is_empty();
		if this.contains(doc_id) && other.contains(doc_id) {
			// A sum of 0 moves to ±1 in this direction.
			other.remove(doc_id);
		} else {
			// No change, or one in the opposite direction, moves to ±1 or to 0.
			// One already in this direction stays where it is: the clamp.
			this.insert(doc_id);
		}
		first
	}

	/// Folds `other`, recorded after `self`, into `self`: each document's sum is
	/// the clamped sum of the two, and a document either one names stays named.
	/// Its postings replace these, so a document `other` names keeps the posting
	/// `other` holds for it, or none.
	fn apply(&mut self, other: &Self) {
		self.apply_membership(other);
		// A document `other` names with no posting of its own changed last in
		// `other` without one, so no earlier posting of it holds.
		if !self.postings.is_empty() {
			for doc_id in other.added.iter().chain(other.removed.iter()) {
				if other.posting_at(doc_id).is_err() {
					self.set_posting(doc_id, 0, 0);
				}
			}
		}
		for (doc_id, frequency, length) in &other.postings {
			self.set_posting(*doc_id, *frequency, *length);
		}
	}

	/// Where document `doc_id`'s entry is in `postings`, or where it would go.
	fn posting_at(&self, doc_id: u64) -> Result<usize, usize> {
		match self.postings.last() {
			// A bulk write records documents in ascending order, each past the last.
			Some((last, ..)) if *last < doc_id => Err(self.postings.len()),
			Some(_) => self.postings.binary_search_by_key(&doc_id, |(posted, ..)| *posted),
			None => Err(0),
		}
	}

	/// Makes `frequency` and `length` document `doc_id`'s last posting; a
	/// frequency of zero leaves it none.
	fn set_posting(&mut self, doc_id: u64, frequency: u32, length: u32) {
		match self.posting_at(doc_id) {
			Ok(at) => self.postings[at] = (doc_id, frequency, length),
			// A document with no entry needs none to say it has no posting.
			Err(_) if frequency == 0 => {}
			Err(at) => self.postings.insert(at, (doc_id, frequency, length)),
		}
	}

	/// [`Self::apply`] for the document sets alone, leaving the postings.
	fn apply_membership(&mut self, other: &Self) {
		if !other.nid.is_nil() {
			self.nid = other.nid;
		}
		// A transaction's own view of a term folds every frame into an empty
		// delta, and the first can name every document of a bulk write.
		if self.is_empty() {
			self.added |= &other.added;
			self.removed |= &other.removed;
			return;
		}
		let pos = &self.added - &self.removed;
		let neg = &self.removed - &self.added;
		let other_pos = &other.added - &other.removed;
		let other_neg = &other.removed - &other.added;
		// A sum of +1 is +1 beside 0 or nothing, either way round, or +1 beside
		// +1 clamped; −1 alike. Every other document either side names sums to 0.
		let sum_pos = (&other_pos - &neg) | (&pos - &other_neg);
		let sum_neg = (&other_neg - &pos) | (&neg - &other_pos);
		// The two are disjoint, so `added` is every named document but those
		// summing to −1, and `removed` every one but those summing to +1.
		let mut named = &self.added | &self.removed;
		named |= &other.added;
		named |= &other.removed;
		self.added = &named - &sum_neg;
		self.removed = named - sum_pos;
	}

	/// The postings of the documents `added` holds whose last change carried
	/// one, in ascending document order.
	pub fn current_postings(&self) -> impl Iterator<Item = (u64, u64, u64)> + '_ {
		self.postings
			.iter()
			.filter(|(doc_id, frequency, _)| *frequency > 0 && self.added.contains(*doc_id))
			.map(|(doc_id, frequency, length)| (*doc_id, u64::from(*frequency), u64::from(*length)))
	}

	fn is_empty(&self) -> bool {
		self.added.is_empty() && self.removed.is_empty()
	}
}

/// Folds the term changes of `frame` into the enclosing `into`. A term `into`
/// holds no change of takes `frame`'s delta as it is.
fn fold_term_changes(
	into: &mut HashMap<BufferedTerm, FullTextDelta>,
	frame: HashMap<BufferedTerm, FullTextDelta>,
) {
	for (term, delta) in frame {
		match into.entry(term) {
			Entry::Occupied(mut held) => held.get_mut().apply(&delta),
			Entry::Vacant(slot) => {
				slot.insert(delta);
			}
		}
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
	/// Adjacency scopes that asked for a fold, and the node id to tag the
	/// queue entry with.
	graph_folds: Mutex<Vec<HashMap<GraphFoldScope, Uuid>>>,
	/// Pending-state guard shards this transaction wrote a pending update
	/// into, to be armed when it commits.
	pending_guards: Mutex<Vec<HashMap<(GuardedIndex, u16), ()>>>,
	/// Indexes whose data this transaction wiped, guards included. Not
	/// save-point scoped: what is known of a wiped index's guards is forgotten
	/// at commit, and forgetting it needlessly costs only a read.
	guard_wipes: Mutex<HashSet<GuardedIndex>>,
	/// Indexes whose pending-state guards this transaction read, noted only on
	/// a store that validates every read. Not save-point scoped: a rolled-back
	/// read may still be validated, and treating an index as read needlessly
	/// costs only a guard write at commit.
	guards_read: Mutex<HashSet<GuardedIndex>>,
}

impl Default for IndexDeltaBuffer {
	fn default() -> Self {
		Self {
			counts: Mutex::new(vec![HashMap::new()]),
			compactions: Mutex::new(vec![HashMap::new()]),
			term_changes: Mutex::new(vec![HashMap::new()]),
			doc_stats: Mutex::new(vec![HashMap::new()]),
			graph_folds: Mutex::new(vec![HashMap::new()]),
			pending_guards: Mutex::new(vec![HashMap::new()]),
			guard_wipes: Mutex::new(HashSet::new()),
			guards_read: Mutex::new(HashSet::new()),
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

	/// Whether an open frame of this transaction has buffered a compaction
	/// trigger for `index`.
	pub fn has_compaction_trigger(&self, index: &BufferedIndex) -> bool {
		self.compactions.lock().iter().any(|frame| frame.contains_key(index))
	}

	/// Record that an adjacency scope wants folding once this transaction
	/// commits. Deduplicated per scope: the queue only needs to name the
	/// scope once per transaction, not once per tombstoned edge.
	// A record id key can embed a regex, which carries interior mutability
	// the hash cannot observe; the same engine-wide reasoning as the idx
	// crate's blanket allow.
	#[allow(clippy::mutable_key_type)]
	pub fn buffer_graph_fold_trigger(&self, scope: GraphFoldScope, nid: Uuid) {
		let mut frames = self.graph_folds.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		frame.insert(scope, nid);
	}

	/// Record that `doc_id` gained (`add`) or lost a full-text term.
	///
	/// `posting` is `Some` on an index that keeps its postings in blocks, and
	/// `None` on any other. For an addition it is the term's frequency in the
	/// document and the document's length. A change that carries none, a
	/// removal, passes a frequency of zero, which no real posting has.
	///
	/// A document's changes sum, as [`FullTextDelta`] describes: one removed and
	/// then re-indexed within one transaction keeps the membership it had, and is
	/// flushed in both directions so the uncompacted changes still name it.
	///
	/// Returns how many keys the flush will write that it would not have
	/// written before this change, *within the frame it lands in*, which is
	/// what the caller charges the write-cardinality guard on: the flush emits
	/// one key per (term, direction) whose set is non-empty. An addition's
	/// postings travel in its own key.
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
		posting: Option<(u64, u64)>,
		nid: Uuid,
	) -> u32 {
		let mut frames = self.term_changes.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		let delta = frame.entry(term).or_default();
		delta.nid = nid;
		// A direction emptied again by a later change keeps the charge it
		// already took, over-counting by one in that case — the safe direction
		// for a limit.
		let first = u32::from(delta.record(doc_id, add));
		match posting {
			Some((frequency, length)) if add && frequency > 0 => {
				// A posting too large to hold is kept as none, so its document is
				// read from what it stores.
				let (frequency, length) = match (u32::try_from(frequency), u32::try_from(length)) {
					(Ok(frequency), Ok(length)) => (frequency, length),
					_ => (0, 0),
				};
				delta.set_posting(doc_id, frequency, length);
				first
			}
			// A change carrying no posting on an index that keeps them: an
			// earlier posting of the document no longer holds.
			Some(_) => {
				delta.set_posting(doc_id, 0, 0);
				first
			}
			None => first,
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
				merged.apply_membership(delta);
			}
		}
		(merged.added, merged.removed)
	}

	/// This transaction's own net change to document `doc_id`'s membership of one
	/// term: +1 where it gave the document the term, −1 where it took the term
	/// away, and 0 where it did neither or did both.
	///
	/// The change [`Self::pending_term_change`] names for the document, read
	/// without merging the term's sets: the frames' changes sum in the order the
	/// frames were opened, held to −1..=+1 at each step as that fold holds them.
	pub fn pending_term_change_of(&self, term: &BufferedTerm, doc_id: u64) -> i64 {
		let mut net = 0i64;
		for frame in self.term_changes.lock().iter() {
			if let Some(delta) = frame.get(term) {
				let change = i64::from(delta.added.contains(doc_id))
					- i64::from(delta.removed.contains(doc_id));
				net = (net + change).clamp(-1, 1);
			}
		}
		net
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

	/// Drain the accumulated term changes.
	///
	/// A document whose changes summed to zero is flushed in both directions
	/// rather than dropped, so the term's uncompacted changes name it.
	pub fn take_term_changes(&self) -> Vec<(BufferedTerm, FullTextDelta)> {
		let mut frames = self.term_changes.lock();
		let merged = {
			let mut drained = frames.drain(..);
			let mut merged = drained.next().unwrap_or_default();
			for frame in drained {
				fold_term_changes(&mut merged, frame);
			}
			merged
		};
		frames.push(HashMap::new());
		merged.into_iter().filter(|(_, delta)| !delta.is_empty()).collect()
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

	/// Record that this transaction wrote a pending update into `shard` of
	/// `index`, so the shard's guard is armed when it commits.
	pub fn buffer_pending_guard(&self, index: GuardedIndex, shard: u16) {
		let mut frames = self.pending_guards.lock();
		let frame = frames.last_mut().expect("the transaction frame is always present");
		frame.insert((index, shard), ());
	}

	/// The shards of an index this transaction has written a pending update
	/// into that a rollback has not undone, as a bit per shard, so a lookup in
	/// it scans those shards whatever their stored guards say.
	pub fn pending_guard_shards(&self, index: &GuardedIndex) -> u64 {
		self.pending_guards
			.lock()
			.iter()
			.flat_map(|frame| frame.keys())
			.filter(|(buffered, _)| buffered == index)
			.fold(0, |shards, (_, shard)| shards | 1 << shard)
	}

	/// Record that this transaction wiped the data of an index, guards
	/// included.
	pub fn buffer_guard_wipe(&self, index: GuardedIndex) {
		self.guard_wipes.lock().insert(index);
	}

	/// Record that this transaction read the index's pending-state guards.
	pub fn note_guards_read(&self, index: GuardedIndex) {
		self.guards_read.lock().insert(index);
	}

	/// Drain the indexes whose pending-state guards this transaction read.
	pub fn take_guards_read(&self) -> HashSet<GuardedIndex> {
		std::mem::take(&mut *self.guards_read.lock())
	}

	/// Whether any pending-state guard shard is waiting to be armed, or any
	/// index's guards were wiped.
	pub fn has_pending_guards(&self) -> bool {
		self.pending_guards.lock().iter().any(|frame| !frame.is_empty())
			|| !self.guard_wipes.lock().is_empty()
	}

	/// Drain the indexes whose guards were wiped.
	pub fn take_guard_wipes(&self) -> Vec<GuardedIndex> {
		self.guard_wipes.lock().drain().collect()
	}

	/// Drain the pending-state guard shards to arm.
	pub fn take_pending_guards(&self) -> Vec<(GuardedIndex, u16)> {
		let mut frames = self.pending_guards.lock();
		let mut merged: HashMap<(GuardedIndex, u16), ()> = HashMap::new();
		for frame in frames.drain(..) {
			merged.extend(frame);
		}
		frames.push(HashMap::new());
		merged.into_keys().collect()
	}

	/// Drain the accumulated fold triggers.
	#[allow(clippy::mutable_key_type)]
	pub fn take_graph_folds(&self) -> Vec<(GraphFoldScope, Uuid)> {
		let mut frames = self.graph_folds.lock();
		let mut merged: HashMap<GraphFoldScope, Uuid> = HashMap::new();
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
			&& self.graph_folds.lock().iter().all(HashMap::is_empty)
			&& self.pending_guards.lock().iter().all(HashMap::is_empty)
			&& self.guard_wipes.lock().is_empty()
	}

	/// Discard everything buffered. Used when the transaction is cancelled.
	pub fn clear(&self) {
		reset_frames(&mut self.counts.lock());
		reset_frames(&mut self.compactions.lock());
		reset_frames(&mut self.term_changes.lock());
		reset_frames(&mut self.doc_stats.lock());
		reset_frames(&mut self.graph_folds.lock());
		reset_frames(&mut self.pending_guards.lock());
		self.guard_wipes.lock().clear();
		self.guards_read.lock().clear();
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
		self.graph_folds.lock().push(HashMap::new());
		self.pending_guards.lock().push(HashMap::new());
	}

	/// Fold the save point's frame into the one beneath it: its writes survived,
	/// so its buffered contributions belong to the enclosing scope.
	#[allow(clippy::mutable_key_type)]
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
				fold_term_changes(parent, top);
			}
		}
		{
			let mut frames = self.doc_stats.lock();
			if let Some(top) = pop_save_point(&mut frames) {
				let parent = frames.last_mut().expect("popping left at least one frame");
				for (index, (stats, nid)) in top {
					fold_doc_stats(parent, index, stats, nid);
				}
			}
		}
		{
			let mut frames = self.graph_folds.lock();
			if let Some(top) = pop_save_point(&mut frames) {
				let parent = frames.last_mut().expect("popping left at least one frame");
				parent.extend(top);
			}
		}
		let mut frames = self.pending_guards.lock();
		if let Some(top) = pop_save_point(&mut frames) {
			let parent = frames.last_mut().expect("popping left at least one frame");
			parent.extend(top);
		}
	}

	/// Discard the save point's frame: its KV writes were rolled back, so its
	/// buffered contributions describe writes that no longer exist.
	pub fn rollback_save_point(&self) {
		pop_save_point(&mut self.counts.lock());
		pop_save_point(&mut self.compactions.lock());
		pop_save_point(&mut self.term_changes.lock());
		pop_save_point(&mut self.doc_stats.lock());
		pop_save_point(&mut self.graph_folds.lock());
		pop_save_point(&mut self.pending_guards.lock());
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
	fn a_rolled_back_save_point_arms_none_of_its_pending_guards() {
		let buffer = IndexDeltaBuffer::new();
		let index = crate::pending_guard::GuardedIndex {
			family: crate::pending_guard::PendingGuardFamily::Hnsw,
			index: term().index,
		};
		buffer.buffer_pending_guard(index.clone(), 1);
		buffer.push_save_point();
		buffer.buffer_pending_guard(index.clone(), 2);
		let shards = || buffer.pending_guard_shards(&index);
		assert_eq!(shards(), 0b110, "an open save point's write is visible");
		buffer.rollback_save_point();
		assert_eq!(shards(), 0b010, "nothing of the rolled-back write remains");
		buffer.push_save_point();
		buffer.buffer_pending_guard(index, 3);
		buffer.release_save_point();
		let mut armed: Vec<u16> =
			buffer.take_pending_guards().into_iter().map(|(_, shard)| shard).collect();
		armed.sort_unstable();
		assert_eq!(armed, vec![1, 3]);
		assert!(buffer.is_empty());
	}

	/// The postings a flush writes with a term's additions are those of the
	/// changes recorded last, on an index that keeps its postings: a document's
	/// last addition, none after a later removal (even one that sums its changes
	/// to nothing), none after a removal in a save point released into the
	/// transaction, and none from a rolled-back save point.
	#[test]
	fn the_postings_flushed_are_those_of_the_changes_that_won() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();
		let removed = Some((0, 0));
		// One key for the term's additions, which carry their postings.
		assert_eq!(buffer.buffer_term_change(term(), 7, true, Some((1, 10)), nid), 1);
		assert_eq!(buffer.buffer_term_change(term(), 9, true, Some((2, 20)), nid), 0);
		let _ = buffer.buffer_term_change(term(), 5, true, Some((3, 30)), nid);
		let _ = buffer.buffer_term_change(term(), 7, true, Some((4, 40)), nid);
		// Added then removed: the sum is nothing, the document is still named,
		// and its posting no longer holds.
		let _ = buffer.buffer_term_change(term(), 9, false, removed, nid);
		buffer.push_save_point();
		let _ = buffer.buffer_term_change(term(), 5, true, Some((5, 50)), nid);
		buffer.rollback_save_point();
		buffer.push_save_point();
		let _ = buffer.buffer_term_change(term(), 11, true, Some((6, 60)), nid);
		buffer.release_save_point();
		// Added in the transaction, removed in a save point released into it.
		let _ = buffer.buffer_term_change(term(), 21, true, Some((8, 80)), nid);
		buffer.push_save_point();
		let _ = buffer.buffer_term_change(term(), 21, false, removed, nid);
		buffer.release_save_point();
		// Removed, then re-added: the re-addition's posting holds.
		let _ = buffer.buffer_term_change(term(), 23, false, removed, nid);
		let _ = buffer.buffer_term_change(term(), 23, true, Some((9, 90)), nid);
		let changes = buffer.take_term_changes();
		let [(_, delta)] = changes.as_slice() else {
			panic!("one term changed: {}", changes.len());
		};
		assert!(delta.added.contains(9) && delta.removed.contains(9), "9 sums to nothing");
		assert_eq!(
			delta.current_postings().collect::<Vec<_>>(),
			vec![(5, 3, 30), (7, 4, 40), (11, 6, 60), (23, 9, 90)]
		);
	}

	/// A transaction that updates one record over and over keeps one posting
	/// for it, its last, however many times it does.
	#[test]
	fn repeated_updates_of_one_record_keep_one_posting() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();
		for update in 1..=1000u64 {
			let _ = buffer.buffer_term_change(term(), 7, false, Some((0, 0)), nid);
			let _ = buffer.buffer_term_change(term(), 7, true, Some((update, 10 + update)), nid);
		}
		let changes = buffer.take_term_changes();
		let [(_, delta)] = changes.as_slice() else {
			panic!("one term changed: {}", changes.len());
		};
		assert_eq!(delta.postings.len(), 1);
		assert_eq!(delta.current_postings().collect::<Vec<_>>(), vec![(7, 1000, 1010)]);
	}

	#[test]
	fn a_rolled_back_save_point_keeps_none_of_its_deltas() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();

		buffer.push_save_point();
		let _ = buffer.buffer_term_change(term(), 7, true, None, nid);
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
		let _ = buffer.buffer_term_change(term(), 7, true, None, nid);
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
		let _ = buffer.buffer_term_change(term(), 7, true, None, nid);
		buffer.buffer_doc_stats(term().index, stats(100, 1), nid);
		buffer.rollback_save_point();

		// The retry indexes the record it actually stored.
		let _ = buffer.buffer_term_change(term(), 9, true, None, nid);
		buffer.buffer_doc_stats(term().index, stats(40, 1), nid);

		let flushed = buffer.take_term_changes();
		assert_eq!(flushed.len(), 1);
		assert!(!flushed[0].1.added.contains(7), "the abandoned attempt must not contribute");
		assert!(flushed[0].1.added.contains(9));
		assert_eq!(buffer.take_doc_stats()[0].1, stats(40, 1));
	}

	/// A document removed and re-indexed within one transaction keeps the
	/// membership it had, so the term's changes sum to zero for it and no
	/// statistics move. It is flushed in both directions, so the term's
	/// uncompacted changes still name it as changed.
	#[test]
	fn a_remove_then_reindex_nets_to_no_change_but_names_the_document() {
		let buffer = IndexDeltaBuffer::new();
		let nid = Uuid::nil();

		let _ = buffer.buffer_term_change(term(), 7, false, None, nid);
		buffer.buffer_doc_stats(term().index, stats(-100, -1), nid);
		let _ = buffer.buffer_term_change(term(), 7, true, None, nid);
		buffer.buffer_doc_stats(term().index, stats(100, 1), nid);

		let (added, removed) = buffer.pending_term_change(&term());
		assert!(added.contains(7) && removed.contains(7), "a read in the transaction sums to 0");

		let flushed = buffer.take_term_changes();
		assert_eq!(flushed.len(), 1);
		assert!(flushed[0].1.added.contains(7));
		assert!(flushed[0].1.removed.contains(7), "the removal cancels the re-addition");
		assert!(buffer.take_doc_stats().is_empty(), "unchanged statistics need no entry");
	}

	/// The sum of a document's changes, as the flushed pair encodes it.
	fn net(delta: &FullTextDelta, doc_id: u64) -> Option<i64> {
		match (delta.added.contains(doc_id), delta.removed.contains(doc_id)) {
			(false, false) => None,
			(added, removed) => Some(i64::from(added) - i64::from(removed)),
		}
	}

	/// Changes to one document sum in the order they are recorded, and every
	/// document a change names stays named.
	#[test]
	fn changes_to_one_document_sum() {
		let cases: [(&[bool], i64); 6] = [
			(&[true], 1),
			(&[false], -1),
			(&[false, true], 0),
			(&[true, false], 0),
			(&[false, true, false], -1),
			(&[true, false, true], 1),
		];
		for (changes, expected) in cases {
			let mut delta = FullTextDelta::default();
			for add in changes {
				let _ = delta.record(7, *add);
			}
			assert_eq!(net(&delta, 7), Some(expected), "{changes:?}");
		}
	}

	/// A save point folds into its parent by the same sum, whatever the
	/// parent's sum: a scope that removed and re-indexed a document leaves the
	/// parent's sum where it was, and one that reversed the parent's change
	/// leaves 0 rather than its own direction.
	#[test]
	fn a_released_save_point_sums_into_its_parent() {
		let cases: [(&[bool], &[bool], i64); 7] = [
			(&[false], &[true], 0),
			(&[true], &[false], 0),
			(&[false], &[false, true], -1),
			(&[true], &[false, true], 1),
			(&[false, true], &[false], -1),
			(&[false, true], &[false, true], 0),
			(&[], &[false, true], 0),
		];
		for (parent, child, expected) in cases {
			let buffer = IndexDeltaBuffer::new();
			let nid = Uuid::nil();
			for add in parent {
				let _ = buffer.buffer_term_change(term(), 7, *add, None, nid);
			}
			buffer.push_save_point();
			for add in child {
				let _ = buffer.buffer_term_change(term(), 7, *add, None, nid);
			}
			let (added, removed) = buffer.pending_term_change(&term());
			let pending = i64::from(added.contains(7)) - i64::from(removed.contains(7));
			assert_eq!(pending, expected, "pending {parent:?} then {child:?}");
			buffer.release_save_point();
			let flushed = buffer.take_term_changes();
			assert_eq!(flushed.len(), 1);
			assert_eq!(net(&flushed[0].1, 7), Some(expected), "{parent:?} then {child:?}");
		}
	}

	/// One document's change, read alone, is the change the merged sets name for
	/// it, after every sequence of changes to two documents and save points
	/// opened, released and rolled back.
	#[test]
	fn a_documents_change_read_alone_is_the_merged_change() {
		#[derive(Clone, Copy, Debug)]
		enum Step {
			Add(u64),
			Remove(u64),
			Push,
			Release,
			Rollback,
		}
		const STEPS: [Step; 6] = [
			Step::Add(7),
			Step::Remove(7),
			Step::Add(8),
			Step::Push,
			Step::Release,
			Step::Rollback,
		];
		const LENGTH: u32 = 6;
		for mut code in 0..STEPS.len().pow(LENGTH) {
			let buffer = IndexDeltaBuffer::new();
			let mut steps = Vec::new();
			for _ in 0..LENGTH {
				let step = STEPS[code % STEPS.len()];
				code /= STEPS.len();
				steps.push(step);
				match step {
					Step::Add(doc_id) => {
						let _ = buffer.buffer_term_change(term(), doc_id, true, None, Uuid::nil());
					}
					Step::Remove(doc_id) => {
						let _ = buffer.buffer_term_change(term(), doc_id, false, None, Uuid::nil());
					}
					Step::Push => buffer.push_save_point(),
					Step::Release => buffer.release_save_point(),
					Step::Rollback => buffer.rollback_save_point(),
				}
				let (added, removed) = buffer.pending_term_change(&term());
				for doc_id in [7, 8, 9] {
					let merged =
						i64::from(added.contains(doc_id)) - i64::from(removed.contains(doc_id));
					assert_eq!(
						buffer.pending_term_change_of(&term(), doc_id),
						merged,
						"document {doc_id} after {steps:?}"
					);
				}
			}
		}
	}
}
