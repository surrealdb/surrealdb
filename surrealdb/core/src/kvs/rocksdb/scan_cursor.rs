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
//!
//! # Lifetime
//!
//! The handle's `'tx` lifetime is the borrow into the parent
//! `Transaction`. Cursors cannot outlive the transaction at the type
//! level. The iterator inside `ScanState` is `'static`-erased (same
//! pattern as `TransactionInner`'s `tx`/`snapshot`) because Rust can't
//! express the self-referential borrow into the boxed
//! `rocksdb::Transaction` or `Pin<Arc<DB>>` without a helper crate.
//!
//! # Safety vs commit/cancel
//!
//! `commit`/`cancel` need to drop the snapshot (and, for writable
//! transactions, consume the boxed `rocksdb::Transaction`) before any
//! cursor's iterator destructor runs — RocksDB's
//! `rocksdb_iter_destroy` decrements the parent's internal refcount, so
//! freeing it on a dropped parent is use-after-free.
//!
//! Two invariants close that race:
//!
//! 1. `Transaction::cursors_alive` counts live cursors. `open_*_cursor` increments after passing
//!    the `done` check (SeqCst); the cursor's drop guard decrements (Release).
//! 2. `commit`/`cancel` set `done = true` (SeqCst), then `yield_now`-loop until `cursors_alive ==
//!    0` (SeqCst) before consuming `inner`. With SeqCst on both atomics, a cursor that successfully
//!    passes its `done` check has its `cursors_alive` increment globally ordered before commit's
//!    load, so commit will wait for it. Conversely, a cursor that opens after commit's `done` store
//!    sees `done == true` and aborts before allocating its iterator.
//!
//! # Drop ordering on the cursor handle
//!
//! The cursor handle is laid out as:
//!
//! ```text
//! struct RocksDbKeysCursor<'tx> {
//!     tx: &'tx Transaction,
//!     state: ScanState,          // contains iter + reusable buffers
//!     _alive_guard: AliveGuard,  // decrements cursors_alive on drop
//! }
//! ```
//!
//! Fields drop in declaration order (Rust reference). So on cursor
//! drop: `tx` (no-op), then `state` (iterator destroyed, snapshot
//! refcount decremented while parent still alive), then `_alive_guard`
//! (counter decrement → unblocks commit's drain). The iterator is
//! destroyed **before** commit is allowed to proceed; commit can then
//! safely drop the snapshot.

use std::sync::atomic::Ordering;

use rocksdb::{DBRawIteratorWithThreadMode, OptimisticTransactionDB};

use super::{Direction, Transaction};
use crate::kvs::Key;
use crate::kvs::api::{
	BoxFut, KeySpan, KeyValSpan, KeysBatch, ScanCursorKeys, ScanCursorVals, ScanLimit, ValsBatch,
};
use crate::kvs::err::Result;

/// Which underlying object the iterator borrows from. Decided at open
/// time by whether the transaction is writable: writable scans iterate
/// on the rocksdb `Transaction` so pending writes in the same logical
/// tx are visible (via `BaseDeltaIterator`); read-only scans iterate on
/// the underlying database directly, which avoids the BaseDeltaIterator
/// wrapper.
pub(super) enum ScanIter {
	/// Iterator built from `self.db.raw_iterator_opt(ro)`. Borrows from
	/// `self.db`. Used for read-only transactions.
	Db(DBRawIteratorWithThreadMode<'static, OptimisticTransactionDB>),
	/// Iterator built from `inner.tx.raw_iterator_opt(ro)`. Borrows from
	/// `inner.tx`. Used for writable transactions so the iterator sees
	/// pending writes in the same logical tx.
	Tx(
		DBRawIteratorWithThreadMode<
			'static,
			rocksdb::Transaction<'static, OptimisticTransactionDB>,
		>,
	),
}

/// Per-keys-cursor state. Owned directly by `RocksDbKeysCursor` — no
/// per-transaction map, no per-batch lock lookup.
///
/// Visibility is `pub(in crate::kvs)` so the `cursor_*` helpers in
/// `mod.rs` can reference it in their signatures.
pub(in crate::kvs) struct ScanStateKeys {
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
	pub(super) start: Key,
	/// Upper bound of the original range. Used by the first-time seek
	/// for `Direction::Backward`.
	pub(super) end: Key,
	/// Concatenated key bytes for the most recent batch. Reused across
	/// `next_batch` calls — the underlying allocation persists, only the
	/// contents are replaced.
	pub(super) key_buf: Vec<u8>,
	/// One `KeySpan` per key in `key_buf` for the most recent batch.
	/// Reused across batches.
	pub(super) key_spans: Vec<KeySpan>,
}

/// Per-vals-cursor state. See [`ScanStateKeys`].
pub(in crate::kvs) struct ScanStateVals {
	/// The underlying iterator.
	pub(super) iter: ScanIter,
	/// Iteration direction, fixed at open time.
	pub(super) dir: Direction,
	/// First-batch-not-yet-issued flag; see [`ScanStateKeys::started`].
	pub(super) started: bool,
	/// First-batch skip count; see [`ScanStateKeys::skip`].
	pub(super) skip: u32,
	/// Original range lower bound.
	pub(super) start: Key,
	/// Original range upper bound.
	pub(super) end: Key,
	/// Concatenated key bytes for the most recent batch. Reused.
	pub(super) key_buf: Vec<u8>,
	/// Concatenated value bytes for the most recent batch. Reused.
	pub(super) val_buf: Vec<u8>,
	/// One `KeyValSpan` per pair. Reused across batches.
	pub(super) spans: Vec<KeyValSpan>,
}

/// RAII guard that decrements `Transaction::cursors_alive` on drop.
/// Declared as the **last** field of each cursor handle so it drops
/// **after** the iterator (declared earlier in the struct) — see the
/// module-level comment for why ordering matters.
pub(super) struct AliveGuard<'tx> {
	tx: &'tx Transaction,
}

impl<'tx> AliveGuard<'tx> {
	pub(super) fn new(tx: &'tx Transaction) -> Self {
		Self {
			tx,
		}
	}
}

impl Drop for AliveGuard<'_> {
	fn drop(&mut self) {
		// Release: this decrement is paired with the SeqCst load in
		// `Transaction::cancel`/`commit`'s wait loop. Any iterator
		// destructor side-effects on the parent's refcount (which ran
		// just before this drop, because the iterator field is declared
		// earlier in the cursor struct) are guaranteed to be visible to
		// the commit thread once it observes `cursors_alive == 0`.
		self.tx.cursors_alive.fetch_sub(1, Ordering::Release);
	}
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
	pub(super) _alive_guard: AliveGuard<'tx>,
}

/// Caller-owned handle for a key+value scan. See [`RocksDbKeysCursor`].
pub(super) struct RocksDbValsCursor<'tx> {
	pub(super) tx: &'tx Transaction,
	pub(super) state: ScanStateVals,
	pub(super) _alive_guard: AliveGuard<'tx>,
}

impl ScanCursorKeys for RocksDbKeysCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: ScanLimit) -> BoxFut<'s, Result<KeysBatch<'s>>> {
		Box::pin(async move { super::cursor_next_keys(self, limit).await })
	}
}

impl ScanCursorVals for RocksDbValsCursor<'_> {
	fn next_batch<'s>(&'s mut self, limit: ScanLimit) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		Box::pin(async move { super::cursor_next_vals(self, limit).await })
	}
}
