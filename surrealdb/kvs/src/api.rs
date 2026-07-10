//! This module defines the API for a transaction in a key-value store.
#![warn(clippy::missing_docs_in_private_items)]

use std::future::Future;
use std::pin::Pin;

use tracing::instrument;

use crate::consts::{COUNT_BATCH_SIZE, NORMAL_BATCH_SIZE};
use crate::cursor::{DefaultKeysCursor, DefaultValsCursor};
use crate::err::{Error, Result};
use crate::timestamp::{
	BoxTimeStamp, BoxTimeStampImpl, HlcTimeStamp, HlcTimeStampImpl, IncTimeStamp, IncTimeStampImpl,
};
use crate::types::{Key, KeyRange};
use crate::{Direction, Val};

/// A boxed future returned by `Transactable` / `ScanCursorKeys` /
/// `ScanCursorVals` trait methods. `Send` only on non-WASM targets — mirrors
/// the `?Send` async-trait variant used previously.
///
/// The bound matches the trait's `TransactionRequirements`
/// (`Send + Sync` natively, empty on WASM), so a `BoxFut` returned from a
/// trait method satisfies whatever the caller expects.
#[cfg(target_family = "wasm")]
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
/// A boxed future returned by `Transactable` / `ScanCursorKeys` /
/// `ScanCursorVals` trait methods.
#[cfg(not(target_family = "wasm"))]
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The result of a [`Transactable::keys`] or [`Transactable::keysr`] operation.
///
/// Contains the fetched keys together with the total number of key bytes in
/// the result, accumulated by the backend during the same iteration that
/// produced the keys.
#[derive(Debug, Default)]
pub struct KeysResult {
	/// The fetched keys.
	pub keys: Vec<Vec<u8>>,
	/// The total number of key bytes in the result.
	pub key_bytes: u64,
}

/// The result of a [`Transactable::scan`] or [`Transactable::scanr`] operation.
///
/// Contains the fetched key-value pairs together with the total number of
/// key and value bytes consumed during the scan. Backends accumulate the
/// counters while iterating their underlying cursor, so callers do not need
/// to make a second pass over the result to compute them.
#[derive(Debug, Default)]
pub struct ScanResult {
	/// The fetched key-value pairs.
	pub values: Vec<(Vec<u8>, Val)>,
	/// The total number of key bytes in the result.
	pub key_bytes: u64,
	/// The total number of value bytes in the result.
	pub value_bytes: u64,
}

/// Per-chunk statistics returned by [`ScanCursorVals::for_each`] /
/// [`ScanCursorKeys::for_each`].
///
/// Accumulated by the backend while driving the visitor, so the caller can
/// update scan metrics and limit accounting without a second pass. `rows`
/// counts every row the cursor advanced over and handed to the visitor —
/// including rows the visitor ignored (e.g. pre-decode-filter rejects) — so it
/// matches the `len()` of an equivalent [`ScanCursorVals::next_batch`] batch.
/// For the keys variant `value_bytes` is always `0`.
#[derive(Debug, Default, Clone, Copy)]
pub struct ScanChunkStats {
	/// Number of `(key, value)` pairs (or keys) visited in this chunk.
	pub rows: u64,
	/// Sum of every visited key's length in bytes.
	pub key_bytes: u64,
	/// Sum of every visited value's length in bytes (`0` for keys scans).
	pub value_bytes: u64,
}

/// The result of a [`Transactable::getm`] operation.
///
/// `key_bytes` is intentionally omitted: callers pass the input keys in and
/// already know their total length, so re-counting them here would be
/// duplicate work on the hot path.
#[derive(Debug, Default)]
pub struct GetMultiResult {
	/// One entry per input key, preserving input order. `None` indicates a
	/// miss.
	pub values: Vec<Option<Val>>,
	/// The number of input keys that were found (count of `Some` entries).
	pub records: u64,
	/// The total number of value bytes across the `Some` entries.
	pub value_bytes: u64,
}

pub mod requirements {
	//! This module defines the trait requirements for a transaction.
	//!
	//! The reason this exists is to allow for swapping out the `Send`
	//! requirement for WASM targets, where we don't want to require `Send` for
	//! transactions. But for non-WASM targets, we do want to require `Send`
	//! for transactions.
	//!
	//! There is no `cfg` / `cfg_attr` support for trait requirements, so we use
	//! this dependent trait to conditionally require `Send` based on the
	//! target family.
	//!
	//! Without this, we would have had to duplicate the entire `Transaction`
	//! trait for WASM and non-WASM targets, which would have been a pain to
	//! maintain.

	/// This trait defines WASM requirements for a transaction.
	#[cfg(target_family = "wasm")]
	pub trait TransactionRequirements {}

	/// Implements the `TransactionRequirements` trait for all types.
	#[cfg(target_family = "wasm")]
	impl<T> TransactionRequirements for T {}

	/// This trait defines non-WASM requirements for a transaction.
	#[cfg(not(target_family = "wasm"))]
	pub trait TransactionRequirements: Send + Sync {}

	/// Implements the `TransactionRequirements` trait for all types that are
	/// `Send`.
	#[cfg(not(target_family = "wasm"))]
	impl<T: Send + Sync> TransactionRequirements for T {}
}

/// Position of a single key inside the cursor's reusable byte buffer.
///
/// Backend-internal: backends fill a `Vec<KeySpan>` while iterating, and
/// `KeysBatch` borrows that vec back out through opaque accessors. Not
/// part of the public cursor API.
///
/// Offsets and lengths are `usize`, matching the buffer's natural
/// addressing. Using `u32` would silently truncate batches whose
/// concatenated bytes exceed 4 GB — possible for batches containing
/// multi-GB RocksDB values, and a defense-in-depth concern even when the
/// numbers are typically small.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
pub struct KeySpan {
	/// Byte offset into the cursor's key buffer.
	pub offset: usize,
	/// Length of this key in bytes.
	pub len: usize,
}

/// Position of a single `(key, value)` pair inside the cursor's reusable
/// key and value byte buffers. See [`KeySpan`] for the layering.
#[derive(Clone, Copy, Debug)]
#[doc(hidden)]
pub struct KeyValSpan {
	/// Byte offset into the cursor's key buffer.
	pub key_offset: usize,
	/// Length of the key in bytes.
	pub key_len: usize,
	/// Byte offset into the cursor's value buffer.
	pub val_offset: usize,
	/// Length of the value in bytes.
	pub val_len: usize,
}

/// Per-batch result of [`ScanCursorKeys::next_batch`], borrowing from the
/// cursor's internal buffer for the duration of the `&mut self` borrow.
///
/// Calling `next_batch` again invalidates the previous batch's slices —
/// the borrow checker enforces this because both batches tie to the same
/// `&'s mut self`.
///
/// **Zero allocations per batch.** The cursor owns a reusable key buffer
/// and span table; this struct is just a borrowed view over them.
/// Iterate via [`Self::iter`] or `&batch` ([`IntoIterator`]); index via
/// [`Self::get`].
pub struct KeysBatch<'c> {
	/// Concatenated key bytes for this batch. Slices over `buf` according
	/// to `spans` produce the individual keys.
	buf: &'c [u8],
	/// `(offset, len)` per key, in scan order.
	spans: &'c [KeySpan],
	/// Sum of every key's length in bytes.
	pub key_bytes: u64,
}

impl<'c> KeysBatch<'c> {
	/// Internal constructor — backends call this with their populated
	/// `buf` and `spans` slices. Not part of the public API.
	#[doc(hidden)]
	#[inline]
	pub fn from_parts(buf: &'c [u8], spans: &'c [KeySpan], key_bytes: u64) -> Self {
		Self {
			buf,
			spans,
			key_bytes,
		}
	}

	/// Number of keys in this batch.
	#[inline]
	pub fn len(&self) -> usize {
		self.spans.len()
	}

	/// `true` when the cursor has reached the end of its range and no
	/// further batches will be produced.
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.spans.is_empty()
	}

	/// Borrowed key at position `i`, or `None` if out of range.
	#[inline]
	pub fn get(&self, i: usize) -> Option<&[u8]> {
		let span = self.spans.get(i)?;
		Some(&self.buf[span.offset..span.offset + span.len])
	}

	/// Iterator over the keys in scan order. Items borrow from the
	/// cursor's internal buffer; they're valid until the next call to
	/// `next_batch` on the same cursor.
	#[inline]
	pub fn iter(&self) -> KeysIter<'_> {
		KeysIter {
			buf: self.buf,
			spans: self.spans.iter(),
		}
	}
}

impl<'a, 'c: 'a> IntoIterator for &'a KeysBatch<'c> {
	type Item = &'a [u8];
	type IntoIter = KeysIter<'a>;
	#[inline]
	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

/// Iterator over the keys in a [`KeysBatch`]. Yields borrowed slices into
/// the cursor's internal buffer.
pub struct KeysIter<'a> {
	/// Concatenated key bytes.
	buf: &'a [u8],
	/// Remaining spans to deliver, in scan order.
	spans: std::slice::Iter<'a, KeySpan>,
}

impl<'a> Iterator for KeysIter<'a> {
	type Item = &'a [u8];
	#[inline]
	fn next(&mut self) -> Option<&'a [u8]> {
		let span = self.spans.next()?;
		Some(&self.buf[span.offset..span.offset + span.len])
	}
	#[inline]
	fn size_hint(&self) -> (usize, Option<usize>) {
		self.spans.size_hint()
	}
}

impl ExactSizeIterator for KeysIter<'_> {}

/// Per-batch result of [`ScanCursorVals::next_batch`], borrowing from the
/// cursor's internal buffers. See [`KeysBatch`].
pub struct ValsBatch<'c> {
	/// Concatenated key bytes for this batch.
	key_buf: &'c [u8],
	/// Concatenated value bytes for this batch.
	val_buf: &'c [u8],
	/// `(key_offset, key_len, value_offset, value_len)` per pair.
	spans: &'c [KeyValSpan],
	/// Sum of every key's length in bytes.
	pub key_bytes: u64,
	/// Sum of every value's length in bytes.
	pub value_bytes: u64,
}

impl<'c> ValsBatch<'c> {
	/// Internal constructor — see [`KeysBatch::from_parts`].
	#[doc(hidden)]
	#[inline]
	pub fn from_parts(
		key_buf: &'c [u8],
		val_buf: &'c [u8],
		spans: &'c [KeyValSpan],
		key_bytes: u64,
		value_bytes: u64,
	) -> Self {
		Self {
			key_buf,
			val_buf,
			spans,
			key_bytes,
			value_bytes,
		}
	}

	/// Number of `(key, value)` pairs in this batch.
	#[inline]
	pub fn len(&self) -> usize {
		self.spans.len()
	}

	/// `true` when the cursor has reached the end of its range.
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.spans.is_empty()
	}

	/// Borrowed `(key, value)` at position `i`, or `None` if out of range.
	#[inline]
	pub fn get(&self, i: usize) -> Option<(&[u8], &[u8])> {
		let span = self.spans.get(i)?;
		let k = &self.key_buf[span.key_offset..span.key_offset + span.key_len];
		let v = &self.val_buf[span.val_offset..span.val_offset + span.val_len];
		Some((k, v))
	}

	/// Iterator over `(key, value)` pairs in scan order.
	#[inline]
	pub fn iter(&self) -> ValsIter<'_> {
		ValsIter {
			key_buf: self.key_buf,
			val_buf: self.val_buf,
			spans: self.spans.iter(),
		}
	}
}

impl<'a, 'c: 'a> IntoIterator for &'a ValsBatch<'c> {
	type Item = (&'a [u8], &'a [u8]);
	type IntoIter = ValsIter<'a>;
	#[inline]
	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

/// Iterator over `(key, value)` pairs in a [`ValsBatch`].
pub struct ValsIter<'a> {
	/// Concatenated key bytes.
	key_buf: &'a [u8],
	/// Concatenated value bytes.
	val_buf: &'a [u8],
	/// Remaining spans to deliver.
	spans: std::slice::Iter<'a, KeyValSpan>,
}

impl<'a> Iterator for ValsIter<'a> {
	type Item = (&'a [u8], &'a [u8]);
	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		let span = self.spans.next()?;
		let k = &self.key_buf[span.key_offset..span.key_offset + span.key_len];
		let v = &self.val_buf[span.val_offset..span.val_offset + span.val_len];
		Some((k, v))
	}
	#[inline]
	fn size_hint(&self) -> (usize, Option<usize>) {
		self.spans.size_hint()
	}
}

impl ExactSizeIterator for ValsIter<'_> {}

/// Per-`(key, value)` visitor for [`ScanCursorVals::for_each`].
///
/// Invoked once per row with slices borrowed directly from the cursor's
/// engine-native handle (no per-row allocation). Return
/// `ControlFlow::Break(())` to stop the scan early (e.g. a consumer-side
/// `LIMIT`) — this does **not** mark the range exhausted, so the cursor stays
/// resumable. The closure must be synchronous (no `.await`).
///
/// `Send` is required only on non-WASM targets, mirroring [`BoxFut`]: a
/// `for_each` future is `Send` there and captures the `&mut dyn ValVisitor`.
#[cfg(not(target_family = "wasm"))]
pub trait ValVisitor: FnMut(&[u8], &[u8]) -> Result<std::ops::ControlFlow<()>> + Send {}
#[cfg(not(target_family = "wasm"))]
impl<T: FnMut(&[u8], &[u8]) -> Result<std::ops::ControlFlow<()>> + Send> ValVisitor for T {}
/// Per-`(key, value)` visitor for [`ScanCursorVals::for_each`]. See the
/// non-WASM definition for the contract.
#[cfg(target_family = "wasm")]
pub trait ValVisitor: FnMut(&[u8], &[u8]) -> Result<std::ops::ControlFlow<()>> {}
#[cfg(target_family = "wasm")]
impl<T: FnMut(&[u8], &[u8]) -> Result<std::ops::ControlFlow<()>>> ValVisitor for T {}

/// Per-key visitor for [`ScanCursorKeys::for_each`]. See [`ValVisitor`] for the
/// contract (this variant receives only the key).
#[cfg(not(target_family = "wasm"))]
pub trait KeyVisitor: FnMut(&[u8]) -> Result<std::ops::ControlFlow<()>> + Send {}
#[cfg(not(target_family = "wasm"))]
impl<T: FnMut(&[u8]) -> Result<std::ops::ControlFlow<()>> + Send> KeyVisitor for T {}
/// Per-key visitor for [`ScanCursorKeys::for_each`].
#[cfg(target_family = "wasm")]
pub trait KeyVisitor: FnMut(&[u8]) -> Result<std::ops::ControlFlow<()>> {}
#[cfg(target_family = "wasm")]
impl<T: FnMut(&[u8]) -> Result<std::ops::ControlFlow<()>>> KeyVisitor for T {}

/// A stateful keys-only scan cursor. Returned by [`Transactable::open_keys_cursor`].
///
/// A cursor represents one logical scan operation (e.g. an outer table walk,
/// or one prefix of a graph-edge traversal). It is opened once and pumped
/// until the range is exhausted or the caller drops the handle, via either of
/// two driving methods (the direction and range bounds are fixed at open
/// time):
///
/// - [`Self::next_batch`] materialises a [`KeysBatch`] borrowed from the cursor for the duration of
///   the call's `&mut self` lifetime — no per-item allocation on the hot path; only one `Vec<_>`
///   allocation per batch. An empty batch signals end of range.
/// - [`Self::for_each`] is the zero-copy streaming alternative: it drives a visitor directly over
///   the same borrowed keys, skipping even the per-batch `Vec`.
///
/// Pick **one** driving method per cursor: interleaving `next_batch` and
/// `for_each` on the same cursor is unsupported (an implementation may buffer
/// rows for one path that the other does not see) and is debug-asserted
/// against by the default cursor.
///
/// Backends that can keep an underlying iterator alive across batches (e.g.
/// RocksDB's `DBRawIterator`) hold it inside the cursor — the caller's
/// `Drop` is what frees the iterator, **not** an LRU. This avoids the
/// thrashing failure mode of a bounded cache when there are more concurrent
/// prefixes than cache slots (e.g. `SELECT ->knows FROM person` with many
/// outer rows).
pub trait ScanCursorKeys: requirements::TransactionRequirements {
	/// Advance the cursor and return up to `limit` more keys, borrowed
	/// from the cursor's internal buffer. An empty batch signals end of
	/// range. The cursor remains valid after an empty batch and may be
	/// dropped at any time.
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<KeysBatch<'s>>>;

	/// Drive the cursor, invoking `f` for up to `limit` keys borrowed directly
	/// from the cursor — the zero-copy streaming path. `f` returns
	/// `ControlFlow::Break(())` to stop early without exhausting the range.
	/// Returns this chunk's row/byte stats (`value_bytes` is always `0`); rows
	/// are counted only after the visitor accepts them, and pre-decode-filter
	/// rejects (visitor returns `Continue`) count as scanned. After a visitor
	/// error the cursor's resume position is implementation-defined — treat
	/// the error as terminal and do not reuse the cursor.
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn KeyVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>>;
}

/// A stateful key+value scan cursor. Returned by [`Transactable::open_vals_cursor`].
///
/// A cursor represents one logical scan operation (e.g. an outer table walk,
/// or one prefix of a graph-edge traversal). It is opened once and pumped
/// until the range is exhausted or the caller drops the handle, via either of
/// two driving methods (the direction and range bounds are fixed at open
/// time):
///
/// - [`Self::next_batch`] materialises a [`ValsBatch`] borrowed from the cursor for the duration of
///   the call's `&mut self` lifetime — no per-item allocation on the hot path; only one `Vec<_>`
///   allocation per batch. An empty batch signals end of range.
/// - [`Self::for_each`] is the zero-copy streaming alternative: it drives a visitor directly over
///   the same borrowed `(key, value)` pairs, skipping even the per-batch `Vec`.
///
/// Pick **one** driving method per cursor: interleaving `next_batch` and
/// `for_each` on the same cursor is unsupported (an implementation may buffer
/// rows for one path that the other does not see) and is debug-asserted
/// against by the default cursor.
///
/// Backends that can keep an underlying iterator alive across batches (e.g.
/// RocksDB's `DBRawIterator`) hold it inside the cursor — the caller's
/// `Drop` is what frees the iterator, **not** an LRU. This avoids the
/// thrashing failure mode of a bounded cache when there are more concurrent
/// prefixes than cache slots (e.g. `SELECT ->knows FROM person` with many
/// outer rows).
pub trait ScanCursorVals: requirements::TransactionRequirements {
	/// Advance the cursor and return up to `limit` more `(key, value)`
	/// pairs, borrowed from the cursor's internal buffer. An empty batch
	/// signals end of range.
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<ValsBatch<'s>>>;

	/// Drive the cursor, invoking `f` for up to `limit` `(key, value)` pairs
	/// borrowed directly from the cursor — the zero-copy streaming path. `f`
	/// returns `ControlFlow::Break(())` to stop early without exhausting the
	/// range. Returns this chunk's row/byte stats; rows are counted only after
	/// the visitor accepts them, and pre-decode-filter rejects (visitor returns
	/// `Continue`) count as scanned. After a visitor error the cursor's resume
	/// position is implementation-defined — treat the error as terminal and do
	/// not reuse the cursor.
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>>;
}

/// A batch scan result returned from the [`Transaction::batch`] or
/// [`Transactor::batch`] functions.
#[derive(Debug)]
pub struct Batch<T> {
	pub next: Option<KeyRange<'static>>,
	pub result: Vec<T>,
}

impl<T> Batch<T> {
	/// Create a new batch scan result.
	pub fn new(next: Option<KeyRange<'static>>, result: Vec<T>) -> Self {
		Self {
			next,
			result,
		}
	}
}

/// A batch which contains only keys
type KeyBatch = Batch<Vec<u8>>;
/// A batch which contains both keys and values.
type ValueBatch = Batch<(Vec<u8>, Vec<u8>)>;

/// This trait defines the API for a transaction in a key-value store.
///
/// All keys and values are represented as byte arrays, encoding is handled
/// one layer up, by the caller.
#[allow(dead_code, reason = "Not used when none of the storage backends are enabled.")]
pub trait Transactable: requirements::TransactionRequirements {
	/// Get the name of the transaction type.
	fn kind(&self) -> &'static str;

	/// Check if transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`crate::err::Error::TransactionFinished`] error.
	fn closed(&self) -> bool;

	/// Check if transaction is writeable.
	///
	/// If the transaction has been marked as a writeable
	/// transaction, then this function will return [`true`].
	/// This fuction can be used to check whether a transaction
	/// allows data to be modified, and if not then the function
	/// will return a [`crate::err::Error::TransactionReadonly`] error.
	fn writeable(&self) -> bool;

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	fn cancel<'a>(&'a self) -> BoxFut<'a, Result<()>>;

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	fn commit<'a>(&'a self) -> BoxFut<'a, Result<()>>;

	/// Check if a key exists in the datastore.
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<bool>>;

	/// Fetch a key from the datastore.
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>>;

	/// Insert or update a key in the datastore.
	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>>;

	/// Insert a key if it doesn't exist in the datastore.
	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>>;

	/// Update a key in the datastore if the current value matches a condition.
	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, Result<()>>;

	/// Delete a key from the datastore.
	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>>;

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>>;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore. Implementations also return the
	/// total number of key bytes scanned, accumulated during the same
	/// iteration that produces the keys, so callers can record metrics
	/// without re-walking the result.
	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>>;

	/// Retrieve a specific range of keys from the datastore, in reverse order.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore. Implementations also return the
	/// total number of key bytes scanned, accumulated during the same
	/// iteration that produces the keys, so callers can record metrics
	/// without re-walking the result.
	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>>;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore. Implementations also return the
	/// total number of value bytes scanned, accumulated during the same
	/// iteration that produces the values, so callers can record metrics
	/// without re-walking the result.
	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>>;

	/// Retrieve a specific range of keys from the datastore in reverse order.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore. Implementations also return the
	/// total number of value bytes scanned, accumulated during the same
	/// iteration that produces the values, so callers can record metrics
	/// without re-walking the result.
	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>>;

	/// Open a stateful keys-only scan cursor over `rng`.
	///
	/// `skip` is applied once on the first batch (drops the first `skip`
	/// keys at the start of the range, then yields up to `limit`).
	/// Subsequent batches continue from the cursor's current position with
	/// no skip.
	///
	/// The cursor's lifetime is tied to `&self`, so it cannot outlive the
	/// transaction. Backends that can keep an underlying iterator alive
	/// across batches override this to do so; otherwise the default impl
	/// wraps the existing single-shot [`Self::keys`] / [`Self::keysr`] and
	/// advances `range.start` between calls. See [`ScanCursorKeys`] for the
	/// rationale.
	fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorKeys + 'a>>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Fall back to the generic cursor that wraps `keys`/`keysr` and
			// advances `rng.start` between batches. Backends that can keep a
			// native iterator alive across batches override this method.
			Ok(Box::new(DefaultKeysCursor::new(self, rng, dir, version, skip))
				as Box<dyn ScanCursorKeys + 'a>)
		})
	}

	/// Open a stateful key+value scan cursor over `rng`. See
	/// [`Self::open_keys_cursor`] for the semantics.
	fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Fall back to the generic cursor that wraps `scan`/`scanr` and
			// advances `rng.start` between batches. Backends that can keep a
			// native iterator alive across batches override this method.
			Ok(Box::new(DefaultValsCursor::new(self, rng, dir, version, skip))
				as Box<dyn ScanCursorVals + 'a>)
		})
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn replace<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.set(key, val).await })
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn clr<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.del(key).await })
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn clrc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { self.delc(key, chk).await })
	}

	/// Fetch many keys from the datastore.
	///
	/// This function fetches all matching keys pairs from the underlying
	/// datastore concurrently. The returned [`GetMultiResult`] also reports
	/// the number of input keys that were found and the total value bytes
	/// across the hits, accumulated in the same loop that performs the
	/// individual point gets.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")))]
	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Continue with function logic
			let mut out = Vec::with_capacity(keys.len());
			let mut records = 0u64;
			let mut value_bytes = 0u64;
			for key in keys {
				if let Some(val) = self.get(key.as_borrowed(), version).await? {
					records += 1;
					value_bytes += val.len() as u64;
					out.push(Some(val));
				} else {
					out.push(None);
				}
			}
			Ok(GetMultiResult {
				values: out,
				records,
				value_bytes,
			})
		})
	}

	/// Retrieve a range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches. The returned [`ScanResult`] also reports
	/// the total key and value bytes consumed during the scan, accumulated
	/// while merging successive batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn getr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Continue with function logic
			let mut out: Vec<(Vec<u8>, Val)> = vec![];
			let mut key_bytes = 0u64;
			let mut value_bytes = 0u64;
			let mut next = Some(rng);
			while let Some(rng) = next {
				let res = self.batch_keys_vals(rng, NORMAL_BATCH_SIZE, version).await?;
				next = res.next;
				for (k, v) in res.result {
					key_bytes += k.len() as u64;
					value_bytes += v.len() as u64;
					out.push((k, v));
				}
			}
			Ok(ScanResult {
				values: out,
				key_bytes,
				value_bytes,
			})
		})
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn delr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Continue with function logic
			let mut next = Some(rng);
			while let Some(rng) = next {
				let res = self.batch_keys(rng, NORMAL_BATCH_SIZE, None).await?;
				next = res.next;
				for k in res.result {
					self.del(Key::from(k)).await?;
				}
			}
			Ok(())
		})
	}
	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn clrr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Check to see if transaction is writable
			if !self.writeable() {
				return Err(Error::TransactionReadonly);
			}
			// Continue with function logic
			let mut next = Some(rng);
			while let Some(rng) = next {
				let res = self.batch_keys(rng, NORMAL_BATCH_SIZE, None).await?;
				next = res.next;
				for k in res.result {
					self.clr(k.into()).await?;
				}
			}
			Ok(())
		})
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total key count from the underlying datastore
	/// in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn count<'a>(&'a self, rng: KeyRange<'a>, version: Option<u64>) -> BoxFut<'a, Result<usize>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Continue with function logic
			let mut len = 0;
			let mut next = Some(rng);
			while let Some(rng) = next {
				let res = self.batch_keys(rng, COUNT_BATCH_SIZE, version).await?;
				next = res.next;
				len += res.result.len();
			}
			Ok(len)
		})
	}

	// --------------------------------------------------
	// Batch functions
	// --------------------------------------------------

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches keys, in batches, with multiple requests to the
	/// underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn batch_keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeyBatch>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Scan for the next batch (we only need the keys here; the byte
			// total is intended for metrics consumers higher up the stack)
			let res = self.keys(rng.as_borrowed(), batch, 0, version).await?.keys;
			// Short page ⇒ the range is fully consumed; no continuation needed.
			if res.len() < batch as usize && batch > 0 {
				Ok(Batch::new(None, res))
			} else {
				// Full page ⇒ produce a continuation range starting after the
				// last returned key.
				match res.last() {
					Some(k) => Ok(Batch::new(
						Some(KeyRange {
							start: Key::from(k).next(),
							end: rng.end.into_static(),
						}),
						res,
					)),
					// Unreachable: the `len < batch` branch above already
					// handles the empty-result case, so a full page must
					// have at least one element.
					None => Ok(Batch::new(None, res)),
				}
			}
		})
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches key-value pairs, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn batch_keys_vals<'a>(
		&'a self,
		rng: KeyRange<'a>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ValueBatch>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Scan for the next batch (we only need the values here; the byte
			// total is intended for metrics consumers higher up the stack)
			let res = self.scan(rng.as_borrowed(), batch, 0, version).await?.values;
			// Short page ⇒ the range is fully consumed; no continuation needed.
			if res.len() < batch as usize && batch > 0 {
				Ok(Batch::new(None, res))
			} else {
				// Full page ⇒ produce a continuation range starting after the
				// last returned key.
				match res.last() {
					Some((k, _)) => {
						let k = Key::from(k).next();
						Ok(Batch::new(
							Some(KeyRange {
								start: k,
								end: rng.end.as_borrowed().into_static(),
							}),
							res,
						))
					}
					// Unreachable: the `len < batch` branch above already
					// handles the empty-result case.
					None => Ok(Batch::new(None, res)),
				}
			}
		})
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	fn new_save_point(&self) -> BoxFut<'_, Result<()>>;

	/// Release the last save point.
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>>;

	/// Rollback to the last save point.
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>>;

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp.
	///
	/// With the `test-inc-timestamp` feature enabled this returns a
	/// deterministic incrementing counter (`IncTimeStamp`) so tests are
	/// reproducible; in production it returns an HLC timestamp
	/// (`HlcTimeStamp`) that combines wall-clock time with a logical counter
	/// for cluster-wide ordering.
	fn timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		Box::pin(async move {
			if cfg!(feature = "test-inc-timestamp") {
				Ok(BoxTimeStamp::new(IncTimeStamp::next()))
			} else {
				Ok(BoxTimeStamp::new(HlcTimeStamp::next()))
			}
		})
	}

	/// Get the current *safe* (closed) watermark timestamp: a versionstamp at or
	/// below which every committed transaction is final **and** visible on this
	/// node.
	///
	/// The live-query router uses this as the upper bound of what it delivers in
	/// a pass, and as the bound it advances its cursor to, so it never advances
	/// past a commit that could still become visible later with a lower
	/// versionstamp (which would silently drop that notification).
	///
	/// The default returns [`Self::timestamp`], which is correct for any backend
	/// with a single monotonic oracle and synchronous local visibility (mem,
	/// rocksdb, surrealkv): everything below a freshly minted stamp is already
	/// committed and visible. A distributed backend whose commit log is
	/// non-linear (e.g. SurrealDS, where the highest committed timestamp can
	/// float above an unapplied lower one) MUST override this to return a genuine
	/// closed/safe watermark, or the router can miss notifications.
	fn safe_timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		self.timestamp()
	}

	/// Get a handle to the timestamp implementation used by [`Self::timestamp`].
	///
	/// Mirrors the same `test-inc-timestamp` swap: deterministic incrementing
	/// counter in tests, HLC in production. Callers that need to mint many
	/// timestamps in a row can hold the impl and avoid re-dispatching.
	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		if cfg!(feature = "test-inc-timestamp") {
			Box::new(IncTimeStampImpl)
		} else {
			Box::new(HlcTimeStampImpl)
		}
	}

	/// Hint the backend to compact the given range (or the whole keyspace if
	/// `None`).
	///
	/// Default impl returns [`Error::CompactionNotSupported`]; backends
	/// whose storage engine exposes a compaction primitive (e.g. RocksDB)
	/// override this. The call is advisory — callers must not rely on it
	/// for correctness, only for reclaiming space / improving read
	/// performance.
	fn compact<'a>(&'a self, _range: Option<KeyRange<'a>>) -> BoxFut<'a, Result<()>> {
		Box::pin(async move { Err(Error::CompactionNotSupported) })
	}
}
