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

/// Per-keys-cursor state. Owned directly by `RocksDbKeysCursor` — no
/// per-transaction map, no per-batch lock lookup.
///
/// Visibility is `pub(crate)` so the `cursor_*` helpers in
/// `mod.rs` can reference it in their signatures.
pub(crate) struct ScanStateKeys {
	/// The underlying iterator.
	pub(super) iter: ScanIter,
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
