//! Caller-owned scan cursors with lock-free per-batch advance.
//!
//! Each cursor handle (`RocksDbKeysCursor` / `RocksDbValsCursor`) owns
//! its iteration state directly — there is no per-transaction map and no
//! per-batch mutex acquisition on the hot path. The cursor's
//! `next_batch` advances the iterator inline; the only synchronisation
//! is a `Transaction::done` atomic load at entry (SeqCst) so cursors
//! bail cleanly if the transaction is being committed/cancelled.
//!
//! Each `next_batch` returns a [`KeysBatch`] / [`ValsBatch`] borrowing
//! from the cursor's own internal buffer for the duration of the
//! `&mut self` borrow. One allocation per batch (the result `Vec<&[u8]>`);
//! the buffer itself is reused across batches. Per-item heap allocations
//! are zero.

use surrealdb_kvs::api::{
	BoxFut, KeySpan, KeyValSpan, KeyVisitor, KeysBatch, ScanChunkStats, ScanCursorKeys,
	ScanCursorVals, ValVisitor, ValsBatch,
};
use surrealdb_kvs::err::Result;

use super::{Direction, Transaction};
use crate::owned_tx::{OwnedRawDbIter, OwnedRawTxIter};

/// Which underlying object the iterator borrows from. Decided at open
/// time by whether the transaction is writable: writable scans iterate
/// on the rocksdb `Transaction` so pending writes in the same logical
/// tx are visible (via `BaseDeltaIterator`); read-only scans iterate on
/// the underlying database directly, which avoids the BaseDeltaIterator
/// wrapper.
pub(super) enum ScanIter {
	/// Iterator built from `self.db.raw_iterator_opt(ro)`.
	/// Used for read-only transactions.
	Db(OwnedRawDbIter),
	/// Iterator built from `inner.tx.raw_iterator_opt(ro)`.
	/// Used for writable transactions so the iterator sees
	/// pending writes in the same logical tx.
	Tx(OwnedRawTxIter),
}

/// Where a writable-transaction cursor resumes if its iterator has to be
/// rebuilt.
///
/// A savepoint rollback rebuilds the write-batch index that a
/// [`ScanIter::Tx`] iterator points into, so the iterator must not be used
/// afterwards. The cursor notices through `epoch` (compared against the
/// transaction's rollback epoch on each batch) and builds a new iterator. The
/// scan continues in the transaction's post-rollback view, strictly after
/// `last`, the key of the last entry the cursor consumed, whether yielded or
/// skipped. Resuming from where the old iterator stood would be wrong: the
/// rollback can restore an entry the old iterator had stepped over, between
/// `last` and its position or beyond a range it had already run out of.
/// Unused by read-only cursors, whose iterators read the database directly and
/// are unaffected by savepoints.
pub(super) struct TxResume {
	/// The transaction's rollback epoch when the iterator was last built.
	pub(super) epoch: u64,
	/// The version the scan reads at, needed to rebuild its `ReadOptions`.
	pub(super) version: Option<u64>,
	/// Key of the last entry consumed, valid when `has_last`.
	last: Vec<u8>,
	/// Whether any entry has been consumed yet.
	has_last: bool,
}

impl TxResume {
	pub(super) fn new(epoch: u64, version: Option<u64>) -> Self {
		Self {
			epoch,
			version,
			last: Vec::new(),
			has_last: false,
		}
	}

	/// Record `key` as the last entry the cursor consumed.
	pub(super) fn consumed(&mut self, key: &[u8]) {
		self.last.clear();
		self.last.extend_from_slice(key);
		self.has_last = true;
	}

	/// Position a freshly built iterator of a cursor that has already started:
	/// strictly after the last consumed entry, or at the range's near edge if
	/// nothing was consumed. A cursor that has not started needs nothing, as
	/// its first batch seeks.
	pub(super) fn reposition(&self, iter: &mut ScanIter, dir: Direction, start: &[u8], end: &[u8]) {
		let ScanIter::Tx(it) = iter else {
			return;
		};
		match dir {
			Direction::Forward => {
				if self.has_last {
					it.seek(&self.last);
					if it.key() == Some(self.last.as_slice()) {
						it.next();
					}
				} else {
					it.seek(start);
				}
			}
			Direction::Backward => {
				if self.has_last {
					it.seek_for_prev(&self.last);
					if it.key() == Some(self.last.as_slice()) {
						it.prev();
					}
				} else {
					it.seek_for_prev(end);
				}
			}
		}
	}
}

/// Per-keys-cursor state. Owned directly by `RocksDbKeysCursor` — no
/// per-transaction map, no per-batch lock lookup.
///
/// Visibility is `pub(crate)` so the `cursor_*` helpers in
/// `mod.rs` can reference it in their signatures.
pub(crate) struct ScanStateKeys {
	/// The underlying iterator.
	pub(super) iter: ScanIter,
	/// Resume point for rebuilding a writable transaction's iterator.
	pub(super) resume: TxResume,
	/// Iteration direction, fixed at open time.
	pub(super) dir: Direction,
	/// Whether `next_batch` has been called at least once. Controls the
	/// first-time seek — the iterator is *not* seeked at open time, so a
	/// caller that opens a cursor and aborts before pumping pays no LSM
	/// seek cost.
	pub(super) started: bool,
	/// Number of leading items to skip on the first `next_batch`.
	/// Cleared to 0 once the first batch has been issued.
	pub(super) skip: u32,
	/// Lower bound of the original range. Used by the first-time seek
	/// for `Direction::Forward`. The iterator's own
	/// `iterate_lower_bound` is also set to this value, so seeks before
	/// it are no-ops.
	pub(super) start: Vec<u8>,
	/// Upper bound of the original range. Used by the first-time seek
	/// for `Direction::Backward`.
	pub(super) end: Vec<u8>,
	/// Concatenated key bytes for the most recent batch. Reused across
	/// `next_batch` calls — the underlying allocation persists, only the
	/// contents are replaced.
	pub(super) key_buf: Vec<u8>,
	/// One `KeySpan` per key in `key_buf` for the most recent batch.
	/// Reused across batches.
	pub(super) key_spans: Vec<KeySpan>,
}

/// Per-vals-cursor state. See [`ScanStateKeys`].
pub(crate) struct ScanStateVals {
	/// The underlying iterator.
	pub(super) iter: ScanIter,
	/// Resume point for rebuilding a writable transaction's iterator.
	pub(super) resume: TxResume,
	/// Iteration direction, fixed at open time.
	pub(super) dir: Direction,
	/// First-batch-not-yet-issued flag; see [`ScanStateKeys::started`].
	pub(super) started: bool,
	/// First-batch skip count; see [`ScanStateKeys::skip`].
	pub(super) skip: u32,
	/// Original range lower bound.
	pub(super) start: Vec<u8>,
	/// Original range upper bound.
	pub(super) end: Vec<u8>,
	/// Concatenated key bytes for the most recent batch. Reused.
	pub(super) key_buf: Vec<u8>,
	/// Concatenated value bytes for the most recent batch. Reused.
	pub(super) val_buf: Vec<u8>,
	/// One `KeyValSpan` per pair. Reused across batches.
	pub(super) spans: Vec<KeyValSpan>,
}

/// Caller-owned handle for a keys-only scan.
///
/// Field order is load-bearing (see module-level comment): `tx` first
/// (no-op drop), `state` second (drops the iterator while parent
/// snapshot is still alive), `_alive_guard` last (decrement signals
/// `commit`/`cancel` that this cursor has finished tearing down).
pub(super) struct RocksDbKeysCursor<'tx> {
	pub(super) tx: &'tx Transaction,
	pub(super) state: ScanStateKeys,
}

/// Caller-owned handle for a key+value scan. See [`RocksDbKeysCursor`].
pub(super) struct RocksDbValsCursor<'tx> {
	pub(super) tx: &'tx Transaction,
	pub(super) state: ScanStateVals,
}

impl ScanCursorKeys for RocksDbKeysCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<KeysBatch<'s>>> {
		Box::pin(async move { super::cursor_next_keys(self, limit).await })
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn KeyVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move { super::cursor_for_each_keys(self, limit, f).await })
	}
}

impl ScanCursorVals for RocksDbValsCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		Box::pin(async move { super::cursor_next_vals(self, limit).await })
	}

	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move { super::cursor_for_each_vals(self, limit, f).await })
	}
}
