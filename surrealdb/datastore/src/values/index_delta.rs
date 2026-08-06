//! Per-transaction aggregation of count-index deltas and compaction triggers.
//!
//! A `COUNT` index is a delta log rather than a counter: a counted mutation
//! appends a signed `!iu` entry instead of updating one shared key, because
//! blind writes of distinct keys never contend whereas a shared key would put
//! every concurrent transaction touching the index into write-write conflict.
//! The same reasoning applies to the `/!ic` compaction queue.
//!
//! That property is worth keeping, but it does not require one key *per
//! document*. Two transactions never share a key here — the entry is tagged with
//! a per-transaction id — so all of a transaction's mutations to one index can
//! collapse into a single entry carrying the net delta without reintroducing any
//! contention. `CREATE |item:1..=20000|` then writes one `!iu` entry and one
//! `/!ic` entry instead of twenty thousand of each.
//!
//! This buffer holds that aggregate until commit. It mirrors
//! [`crate::values::changefeed::Changefeed`]: accumulate during the
//! transaction, flush inside the committing transaction (so the count stays
//! atomic with the document change), discard on cancel.
//!
//! Reads within the same transaction must still observe their own writes, so
//! the count read path adds [`IndexDeltaBuffer::pending_count`] to the entries
//! it scans.

use std::collections::HashMap;

use parking_lot::Mutex;
use surrealdb_catalog::{DatabaseId, IndexId, NamespaceId};
use surrealdb_expr::val::TableName;
use uuid::Uuid;

/// Identifies one index within the transaction's buffers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BufferedIndex {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: TableName,
	pub ix: IndexId,
}

/// Per-transaction buffer of count deltas and compaction triggers.
#[derive(Default)]
pub struct IndexDeltaBuffer {
	/// Net signed count delta per count index, with the node id to tag the
	/// flushed entry with.
	counts: Mutex<HashMap<BufferedIndex, (i64, Uuid)>>,
	/// Indexes that asked for compaction, and the node id to tag the entry with.
	compactions: Mutex<HashMap<BufferedIndex, Uuid>>,
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
		self.counts.lock().is_empty() && self.compactions.lock().is_empty()
	}

	/// Discard everything buffered. Used when the transaction is cancelled.
	pub fn clear(&self) {
		self.counts.lock().clear();
		self.compactions.lock().clear();
	}
}
