//! This module defines the API for a transaction in a key-value store.
#![warn(clippy::missing_docs_in_private_items)]

use std::future::Future;
use std::ops::Range;
use std::pin::Pin;

use anyhow::bail;

use super::direction::Direction;
use super::err::{Error, Result};
use super::util;
use crate::key::debug::Sprintable;
use crate::kvs::batch::Batch;
use crate::kvs::timestamp::IncTimeStamp;
use crate::kvs::{
	BoxTimeStamp, BoxTimeStampImpl, COUNT_BATCH_SIZE, HlcTimeStamp, HlcTimeStampImpl,
	IncTimeStampImpl, Key, NORMAL_BATCH_SIZE, Val,
};

/// A boxed future returned by `Transactable` / `ScanCursorKeys` /
/// `ScanCursorVals` trait methods. `Send` only on non-WASM targets — mirrors
/// the `?Send` async-trait variant used previously.
///
/// The bound matches the trait's `TransactionRequirements`
/// (`Send + Sync` natively, empty on WASM), so a `BoxFut` returned from a
/// trait method satisfies whatever the caller expects.
#[cfg(target_family = "wasm")]
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;
/// A boxed future returned by `Transactable` / `ScanCursorKeys` /
/// `ScanCursorVals` trait methods.
#[cfg(not(target_family = "wasm"))]
pub(crate) type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Specifies the limit for scan operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanLimit {
	/// Fetch up to the specified number of entries
	Count(u32),
	/// Fetch at least the specified number of bytes
	Bytes(u32),
	/// Fetch at least the specified number of bytes, limited by the specified number of entries
	BytesOrCount(u32, u32),
}

impl From<u32> for ScanLimit {
	fn from(count: u32) -> Self {
		ScanLimit::Count(count)
	}
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
	pub values: Vec<(Key, Val)>,

	/// The total number of key bytes in the result.
	pub key_bytes: u64,

	/// The total number of value bytes in the result.
	pub value_bytes: u64,
}

/// The result of a [`Transactable::keys`] or [`Transactable::keysr`] operation.
///
/// Contains the fetched keys together with the total number of key bytes in
/// the result, accumulated by the backend during the same iteration that
/// produced the keys.
#[derive(Debug, Default)]
pub struct KeysResult {
	/// The fetched keys.
	pub keys: Vec<Key>,

	/// The total number of key bytes in the result.
	pub key_bytes: u64,
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
/// Crate-internal: backends fill a `Vec<KeySpan>` while iterating, and
/// `KeysBatch` borrows that vec back out through opaque accessors. Not
/// part of the public cursor API.
///
/// Offsets and lengths are `usize`, matching the buffer's natural
/// addressing. Using `u32` would silently truncate batches whose
/// concatenated bytes exceed 4 GB — possible for batches containing
/// multi-GB RocksDB values, and a defense-in-depth concern even when the
/// numbers are typically small.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeySpan {
	/// Byte offset into the cursor's key buffer.
	pub(crate) offset: usize,
	/// Length of this key in bytes.
	pub(crate) len: usize,
}

/// Position of a single `(key, value)` pair inside the cursor's reusable
/// key and value byte buffers. See [`KeySpan`] for the layering.
#[derive(Clone, Copy, Debug)]
pub(crate) struct KeyValSpan {
	/// Byte offset into the cursor's key buffer.
	pub(crate) key_offset: usize,
	/// Length of the key in bytes.
	pub(crate) key_len: usize,
	/// Byte offset into the cursor's value buffer.
	pub(crate) val_offset: usize,
	/// Length of the value in bytes.
	pub(crate) val_len: usize,
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
	#[inline]
	pub(crate) fn from_parts(buf: &'c [u8], spans: &'c [KeySpan], key_bytes: u64) -> Self {
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
	#[inline]
	pub(crate) fn from_parts(
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

/// A stateful keys-only scan cursor. Returned by [`Transactable::open_keys_cursor`].
///
/// A cursor represents one logical scan operation (e.g. an outer table walk,
/// or one prefix of a graph-edge traversal). It is opened once and pumped via
/// repeated [`Self::next_batch`] calls until it returns an empty batch (range
/// exhausted) or the caller drops the handle. The direction and range bounds
/// are fixed at open time.
///
/// The returned [`KeysBatch`] borrows from the cursor for the duration of
/// the call's `&mut self` lifetime — there is no per-item allocation on
/// the hot path; only one `Vec<&[u8]>` allocation per batch. Backends
/// that can keep an underlying iterator alive across batches (e.g.
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
	fn next_batch<'s>(&'s mut self, limit: ScanLimit) -> BoxFut<'s, Result<KeysBatch<'s>>>;
}

/// A stateful key+value scan cursor. Returned by [`Transactable::open_vals_cursor`].
///
/// See [`ScanCursorKeys`] for the semantics — this variant yields
/// `(key, value)` pairs instead of keys alone.
pub trait ScanCursorVals: requirements::TransactionRequirements {
	/// Advance the cursor and return up to `limit` more `(key, value)`
	/// pairs, borrowed from the cursor's internal buffer. An empty batch
	/// signals end of range.
	fn next_batch<'s>(&'s mut self, limit: ScanLimit) -> BoxFut<'s, Result<ValsBatch<'s>>>;
}

/// Default keys-cursor implementation: wraps the existing single-shot
/// [`Transactable::keys`]/[`Transactable::keysr`], copies each returned
/// key into a cursor-owned buffer, and hands out borrowed slices into
/// that buffer. Backends without a stateful iterator (mem, surrealkv,
/// tikv, indxdb) inherit this. RocksDB overrides with a path that drives
/// `DBRawIterator` directly without re-seeking.
///
/// # Re-seek cost (default impl only)
///
/// Each `next_batch` call here issues a fresh `keys()` round-trip:
/// every batch pays a re-seek against the underlying storage. For local
/// engines (mem, surrealkv) this is a B-tree lookup; for tikv it is a
/// network round-trip. Backends that can do better should override the
/// `open_keys_cursor` method on `Transactable` to return a stateful
/// cursor (as rocksdb does), keeping the iterator pinned across batches.
///
/// # Exhaustion heuristic
///
/// A count-limited batch returning fewer than `c` items terminates the
/// cursor without an extra round-trip, while byte-limited batches always
/// require one trailing empty call to confirm exhaustion.
///
/// **Backend-specific overrides must implement an equivalent termination
/// signal.** The rocksdb cursor uses iterator exhaustion (`iter.valid()
/// == false`) directly rather than the short-batch heuristic, which is
/// safe because the iterator is pinned across batches. Any new override
/// must either preserve the short-batch heuristic (when wrapping a
/// single-shot scan) or substitute an equivalent — never both, never
/// neither, since `exhausted` is the only thing that prevents an
/// infinite loop on a stale post-range cursor.
struct DefaultKeysCursor<'a, T: ?Sized> {
	/// The backing transaction. Borrowed for the cursor's lifetime so it
	/// cannot outlive the transaction.
	tx: &'a T,
	/// Remaining range to scan. Updated after each batch.
	rng: Range<Key>,
	/// Iteration direction, fixed at open time.
	dir: Direction,
	/// Optional version timestamp for versioned reads.
	version: Option<u64>,
	/// Number of leading items to skip on the first batch. Cleared once
	/// the first batch has been issued.
	skip: u32,
	/// Once true, all subsequent calls return an empty batch without
	/// hitting the backend.
	exhausted: bool,
	/// Concatenated key bytes for the most recent batch. Reused across
	/// batches — capacity persists, contents are replaced.
	key_buf: Vec<u8>,
	/// One `KeySpan` per key in `key_buf` for the most recent batch.
	/// Reused across batches.
	key_spans: Vec<KeySpan>,
}

impl<'a, T: ?Sized> DefaultKeysCursor<'a, T> {
	/// Construct a fresh default cursor. The backing key buffer starts
	/// empty; its capacity grows on the first `next_batch` call and
	/// persists across subsequent batches.
	fn new(tx: &'a T, rng: Range<Key>, dir: Direction, version: Option<u64>, skip: u32) -> Self {
		Self {
			tx,
			rng,
			dir,
			version,
			skip,
			exhausted: false,
			key_buf: Vec::new(),
			key_spans: Vec::new(),
		}
	}
}

impl<T> ScanCursorKeys for DefaultKeysCursor<'_, T>
where
	T: Transactable + ?Sized,
{
	fn next_batch<'s>(&'s mut self, limit: ScanLimit) -> BoxFut<'s, Result<KeysBatch<'s>>> {
		Box::pin(async move {
			self.key_buf.clear();
			self.key_spans.clear();
			if self.exhausted || self.rng.start >= self.rng.end {
				return Ok(KeysBatch::from_parts(&self.key_buf, &self.key_spans, 0));
			}
			let skip = std::mem::take(&mut self.skip);
			let res = match self.dir {
				Direction::Forward => {
					self.tx.keys(self.rng.clone(), limit, skip, self.version).await?
				}
				Direction::Backward => {
					self.tx.keysr(self.rng.clone(), limit, skip, self.version).await?
				}
			};
			// Pre-allocate the backing arena once: one capacity request, then
			// `extend_from_slice` for each key never reallocates because we
			// reserved the total up-front.
			let total_bytes: usize = res.keys.iter().map(|k| k.len()).sum();
			self.key_buf.reserve(total_bytes);
			self.key_spans.reserve(res.keys.len());
			for k in &res.keys {
				let offset = self.key_buf.len();
				let len = k.len();
				self.key_buf.extend_from_slice(k);
				self.key_spans.push(KeySpan {
					offset,
					len,
				});
			}
			match res.keys.last() {
				Some(last) => {
					// Advance the unbounded edge past the last key seen so
					// the next call resumes after this batch.
					//
					// Forward: the minimal key strictly greater than
					// `last` is `last || [\x00]`. We append `\x00` to make
					// the next range `[last\0, end)`, which excludes
					// `last` (already returned) and keeps every key
					// strictly greater — including any key that has
					// `last` as a strict prefix (e.g. `a\0`, `ab`, ...
					// after `a`).
					//
					// We deliberately do NOT use `push(0xff)` (would skip
					// `(last, last\xff)`) nor a byte-level increment
					// like `util::advance_key` (would skip every key
					// that has `last` as a strict prefix — e.g. `a` →
					// `b` jumps past `a\0`, `ab`, ...). Both are wrong
					// for a cursor consumed batch-by-batch over an
					// arbitrary byte range.
					//
					// Backward: the range is `[start, end)`, so clipping
					// `end` to `last` already excludes `last` and keeps
					// every key strictly less than it.
					match self.dir {
						Direction::Forward => {
							self.rng.start.clone_from(last);
							self.rng.start.push(0x00);
						}
						Direction::Backward => {
							self.rng.end.clone_from(last);
						}
					}
					// Count-limited short batch ⇒ definitively exhausted.
					if let ScanLimit::Count(c) = limit
						&& res.keys.len() < c as usize
					{
						self.exhausted = true;
					}
				}
				None => {
					self.exhausted = true;
				}
			}
			Ok(KeysBatch::from_parts(&self.key_buf, &self.key_spans, res.key_bytes))
		})
	}
}

/// Default vals-cursor implementation: see [`DefaultKeysCursor`].
struct DefaultValsCursor<'a, T: ?Sized> {
	/// The backing transaction. Borrowed for the cursor's lifetime.
	tx: &'a T,
	/// Remaining range to scan. Updated after each batch.
	rng: Range<Key>,
	/// Iteration direction, fixed at open time.
	dir: Direction,
	/// Optional version timestamp for versioned reads.
	version: Option<u64>,
	/// Number of leading items to skip on the first batch.
	skip: u32,
	/// Once true, all subsequent calls return an empty batch without
	/// hitting the backend.
	exhausted: bool,
	/// Concatenated key bytes for the most recent batch. Reused.
	key_buf: Vec<u8>,
	/// Concatenated value bytes for the most recent batch. Reused.
	val_buf: Vec<u8>,
	/// One `KeyValSpan` per pair. Reused.
	spans: Vec<KeyValSpan>,
}

impl<'a, T: ?Sized> DefaultValsCursor<'a, T> {
	/// Construct a fresh default cursor. The backing key/value buffers
	/// start empty; their capacity grows on the first `next_batch` call
	/// and persists across subsequent batches.
	fn new(tx: &'a T, rng: Range<Key>, dir: Direction, version: Option<u64>, skip: u32) -> Self {
		Self {
			tx,
			rng,
			dir,
			version,
			skip,
			exhausted: false,
			key_buf: Vec::new(),
			val_buf: Vec::new(),
			spans: Vec::new(),
		}
	}
}

impl<T> ScanCursorVals for DefaultValsCursor<'_, T>
where
	T: Transactable + ?Sized,
{
	fn next_batch<'s>(&'s mut self, limit: ScanLimit) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		Box::pin(async move {
			self.key_buf.clear();
			self.val_buf.clear();
			self.spans.clear();
			if self.exhausted || self.rng.start >= self.rng.end {
				return Ok(ValsBatch::from_parts(&self.key_buf, &self.val_buf, &self.spans, 0, 0));
			}
			let skip = std::mem::take(&mut self.skip);
			let res = match self.dir {
				Direction::Forward => {
					self.tx.scan(self.rng.clone(), limit, skip, self.version).await?
				}
				Direction::Backward => {
					self.tx.scanr(self.rng.clone(), limit, skip, self.version).await?
				}
			};
			let (kb, vb): (usize, usize) =
				res.values.iter().fold((0, 0), |(ka, va), (k, v)| (ka + k.len(), va + v.len()));
			self.key_buf.reserve(kb);
			self.val_buf.reserve(vb);
			self.spans.reserve(res.values.len());
			for (k, v) in &res.values {
				let key_offset = self.key_buf.len();
				let key_len = k.len();
				self.key_buf.extend_from_slice(k);
				let val_offset = self.val_buf.len();
				let val_len = v.len();
				self.val_buf.extend_from_slice(v);
				self.spans.push(KeyValSpan {
					key_offset,
					key_len,
					val_offset,
					val_len,
				});
			}
			match res.values.last() {
				Some((last, _)) => {
					// See `DefaultKeysCursor` for the successor-logic
					// rationale: `push(0x00)` gives the minimal key
					// strictly greater than `last`, so no key in
					// `(last, ...]` is skipped at the batch boundary.
					match self.dir {
						Direction::Forward => {
							self.rng.start.clone_from(last);
							self.rng.start.push(0x00);
						}
						Direction::Backward => {
							self.rng.end.clone_from(last);
						}
					}
					if let ScanLimit::Count(c) = limit
						&& res.values.len() < c as usize
					{
						self.exhausted = true;
					}
				}
				None => {
					self.exhausted = true;
				}
			}
			Ok(ValsBatch::from_parts(
				&self.key_buf,
				&self.val_buf,
				&self.spans,
				res.key_bytes,
				res.value_bytes,
			))
		})
	}
}

/// This trait defines the API for a transaction in a key-value store.
///
/// All keys and values are represented as byte arrays, encoding is handled
/// by [`super::tr::Transactor`].
#[allow(dead_code, reason = "Not used when none of the storage backends are enabled.")]
pub trait Transactable: requirements::TransactionRequirements {
	/// Get the name of the transaction type.
	fn kind(&self) -> &'static str;

	/// Check if transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`crate::kvs::Error::TransactionFinished`] error.
	fn closed(&self) -> bool;

	/// Check if transaction is writeable.
	///
	/// If the transaction has been marked as a writeable
	/// transaction, then this function will return [`true`].
	/// This fuction can be used to check whether a transaction
	/// allows data to be modified, and if not then the function
	/// will return a [`crate::kvs::Error::TransactionReadonly`] error.
	fn writeable(&self) -> bool;

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	fn cancel(&self) -> BoxFut<'_, Result<()>>;

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	fn commit(&self) -> BoxFut<'_, Result<()>>;

	/// Check if a key exists in the datastore.
	fn exists(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<bool>>;

	/// Fetch a key from the datastore.
	fn get(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<Option<Val>>>;

	/// Insert or update a key in the datastore.
	fn set(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>>;

	/// Insert a key if it doesn't exist in the datastore.
	fn put(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>>;

	/// Update a key in the datastore if the current value matches a condition.
	fn putc(&self, key: Key, val: Val, chk: Option<Val>) -> BoxFut<'_, Result<()>>;

	/// Delete a key from the datastore.
	fn del(&self, key: Key) -> BoxFut<'_, Result<()>>;

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	fn delc(&self, key: Key, chk: Option<Val>) -> BoxFut<'_, Result<()>>;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore. Implementations also return the
	/// total number of key bytes scanned, accumulated during the same
	/// iteration that produces the keys, so callers can record metrics
	/// without re-walking the result.
	fn keys(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<KeysResult>>;

	/// Retrieve a specific range of keys from the datastore, in reverse order.
	///
	/// This function fetches the full range of keys without values, in a single
	/// request to the underlying datastore. Implementations also return the
	/// total number of key bytes scanned, accumulated during the same
	/// iteration that produces the keys, so callers can record metrics
	/// without re-walking the result.
	fn keysr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<KeysResult>>;

	/// Retrieve a specific range of keys from the datastore.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore. Implementations also return the
	/// total number of value bytes scanned, accumulated during the same
	/// iteration that produces the values, so callers can record metrics
	/// without re-walking the result.
	fn scan(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<ScanResult>>;

	/// Retrieve a specific range of keys from the datastore in reverse order.
	///
	/// This function fetches the full range of key-value pairs, in a single
	/// request to the underlying datastore. Implementations also return the
	/// total number of value bytes scanned, accumulated during the same
	/// iteration that produces the values, so callers can record metrics
	/// without re-walking the result.
	fn scanr(
		&self,
		rng: Range<Key>,
		limit: ScanLimit,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<ScanResult>>;

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
		rng: Range<Key>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorKeys + 'a>>> {
		Box::pin(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			Ok(Box::new(DefaultKeysCursor::new(self, rng, dir, version, skip))
				as Box<dyn ScanCursorKeys + 'a>)
		})
	}

	/// Open a stateful key+value scan cursor over `rng`. See
	/// [`Self::open_keys_cursor`] for the semantics.
	fn open_vals_cursor<'a>(
		&'a self,
		rng: Range<Key>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		Box::pin(async move {
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			Ok(Box::new(DefaultValsCursor::new(self, rng, dir, version, skip))
				as Box<dyn ScanCursorVals + 'a>)
		})
	}

	/// Insert or replace a key in the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn replace(&self, key: Key, val: Val) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.set(key, val).await })
	}

	/// Delete all versions of a key from the datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn clr(&self, key: Key) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.del(key).await })
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn clrc(&self, key: Key, chk: Option<Val>) -> BoxFut<'_, Result<()>> {
		Box::pin(async move { self.delc(key, chk).await })
	}

	/// Fetch many keys from the datastore.
	///
	/// This function fetches all matching keys pairs from the underlying
	/// datastore concurrently. The returned [`GetMultiResult`] also reports
	/// the number of input keys that were found and the total value bytes
	/// across the hits, accumulated in the same loop that performs the
	/// individual point gets.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(keys = keys.sprint()))]
	fn getm(&self, keys: Vec<Key>, version: Option<u64>) -> BoxFut<'_, Result<GetMultiResult>> {
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
				if let Some(val) = self.get(key, version).await? {
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

	/// Retrieve a range of prefixed keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches. The returned [`ScanResult`] also reports
	/// the total key and value bytes consumed during the scan.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn getp(&self, key: Key, version: Option<u64>) -> BoxFut<'_, Result<ScanResult>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Continue with function logic
			let range = util::to_prefix_range(&key)?;
			self.getr(range, version).await
		})
	}

	/// Retrieve a range of keys from the datastore.
	///
	/// This function fetches all matching key-value pairs from the underlying
	/// datastore in grouped batches. The returned [`ScanResult`] also reports
	/// the total key and value bytes consumed during the scan, accumulated
	/// while merging successive batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn getr(&self, rng: Range<Key>, version: Option<u64>) -> BoxFut<'_, Result<ScanResult>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Continue with function logic
			let mut out: Vec<(Key, Val)> = vec![];
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

	/// Delete a range of prefixed keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn delp(&self, key: Key) -> BoxFut<'_, Result<()>> {
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
			let range = util::to_prefix_range(&key)?;
			self.delr(range).await
		})
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn delr(&self, rng: Range<Key>) -> BoxFut<'_, Result<()>> {
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
					self.del(k).await?;
				}
			}
			Ok(())
		})
	}

	/// Delete all versions of a range of prefixed keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.sprint()))]
	fn clrp(&self, key: Key) -> BoxFut<'_, Result<()>> {
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
			let range = util::to_prefix_range(&key)?;
			self.clrr(range).await
		})
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes all matching key-value pairs from the underlying
	/// datastore in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn clrr(&self, rng: Range<Key>) -> BoxFut<'_, Result<()>> {
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
					self.clr(k).await?;
				}
			}
			Ok(())
		})
	}

	/// Count the total number of keys within a range in the datastore.
	///
	/// This function fetches the total key count from the underlying datastore
	/// in grouped batches.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn count(&self, rng: Range<Key>, version: Option<u64>) -> BoxFut<'_, Result<usize>> {
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
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn batch_keys(
		&self,
		rng: Range<Key>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<Batch<Key>>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Continue with function logic
			let end = rng.end.clone();
			// Scan for the next batch (we only need the keys here; the byte
			// total is intended for metrics consumers higher up the stack)
			let res = self.keys(rng, ScanLimit::Count(batch), 0, version).await?.keys;
			// Check if range is consumed
			if res.len() < batch as usize && batch > 0 {
				Ok(Batch::<Key>::new(None, res))
			} else {
				match res.last() {
					Some(k) => {
						let mut k = k.clone();
						util::advance_key(&mut k);
						Ok(Batch::<Key>::new(
							Some(Range {
								start: k,
								end,
							}),
							res,
						))
					}
					// We have checked the length above, so
					// there should be a last item in the
					// vector, so we shouldn't arrive here
					None => Ok(Batch::<Key>::new(None, res)),
				}
			}
		})
	}

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches key-value pairs, in batches, with multiple
	/// requests to the underlying datastore.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.sprint()))]
	fn batch_keys_vals(
		&self,
		rng: Range<Key>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'_, Result<Batch<(Key, Val)>>> {
		Box::pin(async move {
			// Check to see if transaction is closed
			if self.closed() {
				return Err(Error::TransactionFinished);
			}
			// Continue with function logic
			let end = rng.end.clone();
			// Scan for the next batch (we only need the values here; the byte
			// total is intended for metrics consumers higher up the stack)
			let res = self.scan(rng, ScanLimit::Count(batch), 0, version).await?.values;
			// Check if range is consumed
			if res.len() < batch as usize && batch > 0 {
				Ok(Batch::<(Key, Val)>::new(None, res))
			} else {
				match res.last() {
					Some((k, _)) => {
						let mut k = k.clone();
						util::advance_key(&mut k);
						Ok(Batch::<(Key, Val)>::new(
							Some(Range {
								start: k,
								end,
							}),
							res,
						))
					}
					// We have checked the length above, so
					// there should be a last item in the
					// vector, so we shouldn't arrive here
					None => Ok(Batch::<(Key, Val)>::new(None, res)),
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

	/// Get the current monotonic timestamp
	fn timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		Box::pin(async move {
			if cfg!(test) {
				Ok(BoxTimeStamp::new(IncTimeStamp::next()))
			} else {
				Ok(BoxTimeStamp::new(HlcTimeStamp::next()))
			}
		})
	}

	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		if cfg!(test) {
			Box::new(IncTimeStampImpl)
		} else {
			Box::new(HlcTimeStampImpl)
		}
	}

	fn compact(&self, _range: Option<Range<Key>>) -> BoxFut<'_, anyhow::Result<()>> {
		Box::pin(async move { bail!(Error::CompactionNotSupported) })
	}
}
