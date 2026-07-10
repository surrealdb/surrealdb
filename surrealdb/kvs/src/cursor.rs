//! The default resume-by-bound scan cursor.
//!
//! Backends that cannot keep a native iterator alive across batches (surrealkv,
//! tikv, indxdb — plus the mem cursor's materialising `next_batch` path) drive
//! scans through [`DefaultKeysCursor`] / [`DefaultValsCursor`]. These re-issue a
//! single-shot `scan`/`keys` per page and advance a resume bound (range + skip)
//! between pages, instead of holding a live iterator like RocksDB's cursor.
//!
//! The page-fetch + resume/exhaustion bookkeeping lives in [`fetch_vals_page`] /
//! [`fetch_keys_page`] (also used directly by the zero-copy `for_each` path);
//! [`fill_vals_batch`] / [`fill_keys_batch`] additionally pack a fetched page
//! into the cursor's reusable flat buffers for the materialising `next_batch`
//! path. The cursor traits, batch/visitor types, and `ScanChunkStats` they build
//! on are defined alongside the transaction API in [`super::api`].

use crate::api::{
	BoxFut, KeySpan, KeyValSpan, KeyVisitor, KeysBatch, KeysResult, ScanChunkStats, ScanCursorKeys,
	ScanCursorVals, ScanResult, Transactable, ValVisitor, ValsBatch,
};
use crate::err::Result;
use crate::types::{Key, KeyRange};
use crate::{Direction, Val};

fn update_range(rng: &mut KeyRange<'_>, dir: Direction, last: &[u8]) {
	match dir {
		Direction::Forward => {
			rng.start.clone_from_slice(last);
			rng.start.advance();
		}
		Direction::Backward => rng.end.clone_from_slice(last),
	}
}

/// Fetch one page from the backend's single-shot `scan`/`scanr` in `dir`,
/// advancing the cursor's resume bound past the page and flagging exhaustion
/// (count-limited short page ⇒ end of stream). Shared by the default cursor's
/// `next_batch`/`for_each` and the mem cursor's `next_batch`.
async fn fetch_vals_page<T: Transactable + ?Sized>(
	tx: &T,
	rng: &mut KeyRange<'_>,
	dir: Direction,
	version: Option<u64>,
	skip: &mut u32,
	limit: u32,
) -> Result<ScanResult> {
	if rng.is_empty() {
		return Ok(ScanResult::default());
	}
	let skip = std::mem::take(skip);
	let res = match dir {
		Direction::Forward => tx.scan(rng.as_borrowed(), limit, skip, version).await?,
		Direction::Backward => tx.scanr(rng.as_borrowed(), limit, skip, version).await?,
	};

	if res.values.len() < limit as usize {
		rng.end = Key::empty();
	} else if let Some((last, _)) = res.values.last() {
		update_range(rng, dir, last);
	}
	Ok(res)
}

/// `next_batch` body shared by the default and mem vals cursors: fetch one page
/// via [`fetch_vals_page`] and pack it into the reusable `(key_buf, val_buf,
/// spans)` arenas. Returns the page's `(key_bytes, value_bytes)`; the caller
/// wraps the arenas in a [`ValsBatch`]. (The mem cursor's zero-copy fast path is
/// `for_each`; its `next_batch` — the materialising path — shares this.)
#[allow(clippy::too_many_arguments, reason = "threads the cursor's scan state + reusable arenas")]
pub async fn fill_vals_batch<T: Transactable + ?Sized>(
	tx: &T,
	rng: &mut KeyRange<'_>,
	dir: Direction,
	version: Option<u64>,
	skip: &mut u32,
	key_buf: &mut Vec<u8>,
	val_buf: &mut Vec<u8>,
	spans: &mut Vec<KeyValSpan>,
	limit: u32,
) -> Result<(u64, u64)> {
	key_buf.clear();
	val_buf.clear();
	spans.clear();
	let res = fetch_vals_page(tx, rng, dir, version, skip, limit).await?;
	let (kb, vb): (usize, usize) =
		res.values.iter().fold((0, 0), |(ka, va), (k, v)| (ka + k.len(), va + v.len()));
	key_buf.reserve(kb);
	val_buf.reserve(vb);
	spans.reserve(res.values.len());
	for (k, v) in &res.values {
		let key_offset = key_buf.len();
		let key_len = k.len();
		key_buf.extend_from_slice(k);
		let val_offset = val_buf.len();
		let val_len = v.len();
		val_buf.extend_from_slice(v);
		spans.push(KeyValSpan {
			key_offset,
			key_len,
			val_offset,
			val_len,
		});
	}
	Ok((res.key_bytes, res.value_bytes))
}

/// Keys analogue of [`fetch_vals_page`].
async fn fetch_keys_page<T: Transactable + ?Sized>(
	tx: &T,
	rng: &mut KeyRange<'_>,
	dir: Direction,
	version: Option<u64>,
	skip: &mut u32,
	limit: u32,
) -> Result<KeysResult> {
	if rng.is_empty() {
		return Ok(KeysResult::default());
	}
	let skip = std::mem::take(skip);
	let res = match dir {
		Direction::Forward => tx.keys(rng.as_borrowed(), limit, skip, version).await?,
		Direction::Backward => tx.keysr(rng.as_borrowed(), limit, skip, version).await?,
	};
	match res.keys.last() {
		Some(last) => {
			// A short page means the backend ran out of rows before the
			// requested count: the range is exhausted.
			if res.keys.len() < limit as usize {
				rng.end = Key::empty();
			} else {
				update_range(rng, dir, last);
			}
		}
		None => {
			rng.end = Key::empty();
		}
	}
	Ok(res)
}

/// Keys analogue of [`fill_vals_batch`].
#[allow(clippy::too_many_arguments, reason = "threads the cursor's scan state + reusable arena")]
async fn fill_keys_batch<T: Transactable + ?Sized>(
	tx: &T,
	rng: &mut KeyRange<'_>,
	dir: Direction,
	version: Option<u64>,
	skip: &mut u32,
	key_buf: &mut Vec<u8>,
	key_spans: &mut Vec<KeySpan>,
	limit: u32,
) -> Result<u64> {
	key_buf.clear();
	key_spans.clear();
	let res = fetch_keys_page(tx, rng, dir, version, skip, limit).await?;
	let total_bytes: usize = res.keys.iter().map(|k| k.len()).sum();
	key_buf.reserve(total_bytes);
	key_spans.reserve(res.keys.len());
	for k in &res.keys {
		let offset = key_buf.len();
		let len = k.len();
		key_buf.extend_from_slice(k);
		key_spans.push(KeySpan {
			offset,
			len,
		});
	}
	Ok(res.key_bytes)
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
/// A batch returning fewer items than the requested limit terminates the
/// cursor without an extra round-trip.
///
/// **Backend-specific overrides must implement an equivalent termination
/// signal.** The rocksdb cursor uses iterator exhaustion (`iter.valid()
/// == false`) directly rather than the short-batch heuristic, which is
/// safe because the iterator is pinned across batches. Any new override
/// must either preserve the short-batch heuristic (when wrapping a
/// single-shot scan) or substitute an equivalent — never both, never
/// neither, since `exhausted` is the only thing that prevents an
/// infinite loop on a stale post-range cursor.
pub struct DefaultKeysCursor<'a, T: ?Sized> {
	/// The backing transaction. Borrowed for the cursor's lifetime so it
	/// cannot outlive the transaction.
	tx: &'a T,
	/// Remaining range to scan. Updated after each batch.
	rng: KeyRange<'a>,
	/// Iteration direction, fixed at open time.
	dir: Direction,
	/// Optional version timestamp for versioned reads.
	version: Option<u64>,
	/// Number of leading items to skip on the first batch. Cleared once
	/// the first batch has been issued.
	skip: u32,
	/// Concatenated key bytes for the most recent batch. Reused across
	/// batches — capacity persists, contents are replaced.
	key_buf: Vec<u8>,
	/// One `KeySpan` per key in `key_buf` for the most recent batch.
	/// Reused across batches.
	key_spans: Vec<KeySpan>,
	/// Rows `for_each` fetched into a page but did not visit — kept here so the
	/// next call resumes from the buffer instead of re-scanning them. A page is
	/// only partially drained when a chunk ends mid-page: after an early
	/// `Break`, or when the chunk's `limit` is reached before the buffered page
	/// is exhausted (e.g. a later call with a smaller `limit`).
	///
	/// `next_batch` ignores this buffer (it reads from `rng`), so the buffer
	/// must be empty whenever `next_batch` runs: interleaving the two paths on
	/// one cursor is unsupported and would silently skip these buffered rows.
	/// Debug-asserted at the top of `next_batch`.
	pending: std::vec::IntoIter<Vec<u8>>,
}

impl<'a, T: ?Sized> DefaultKeysCursor<'a, T> {
	/// Construct a fresh default cursor. The backing key buffer starts
	/// empty; its capacity grows on the first `next_batch` call and
	/// persists across subsequent batches.
	pub(crate) fn new(
		tx: &'a T,
		rng: KeyRange<'a>,
		dir: Direction,
		version: Option<u64>,
		skip: u32,
	) -> Self {
		Self {
			tx,
			rng,
			dir,
			version,
			skip,
			key_buf: Vec::new(),
			key_spans: Vec::new(),
			pending: Vec::new().into_iter(),
		}
	}
}

impl<'a, T> ScanCursorKeys for DefaultKeysCursor<'a, T>
where
	T: Transactable + ?Sized,
{
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<KeysBatch<'s>>> {
		Box::pin(async move {
			// `next_batch` reads from `rng`, not from `for_each`'s `pending`
			// buffer; mixing the two paths on one cursor would silently skip
			// any rows `for_each` left buffered (after an early `Break`, or a
			// chunk that hit its `limit` mid-page). No consumer mixes them —
			// assert the invariant in debug builds.
			debug_assert!(
				self.pending.as_slice().is_empty(),
				"next_batch called while for_each left rows buffered (early Break or mid-page limit stop); the two paths must not be mixed on one cursor",
			);
			// Fetch one page and pack it into the reusable arena; the
			// successor/exhaustion bookkeeping lives in `fill_keys_batch`.
			let key_bytes = fill_keys_batch(
				self.tx,
				&mut self.rng,
				self.dir,
				self.version,
				&mut self.skip,
				&mut self.key_buf,
				&mut self.key_spans,
				limit,
			)
			.await?;
			// Hand back borrowed slices into the freshly populated arena.
			Ok(KeysBatch::from_parts(&self.key_buf, &self.key_spans, key_bytes))
		})
	}

	/// Drive the visitor over the next chunk without the second copy that
	/// `next_batch` pays — see [`DefaultValsCursor::for_each`].
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn KeyVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			// Stats accumulate row-by-row (counted only after the visitor
			// accepts each), so an early `Break` reports exactly what was seen.
			let mut stats = ScanChunkStats::default();
			loop {
				// Drain the buffered tail of the previously fetched page first:
				// an earlier `Break` may have left unvisited rows here, which we
				// resume from instead of re-scanning them from the backend.
				while stats.rows < limit as u64 {
					let Some(k) = self.pending.next() else {
						break;
					};
					let flow = f(&k)?;
					stats.rows += 1;
					stats.key_bytes += k.len() as u64;
					if let std::ops::ControlFlow::Break(()) = flow {
						return Ok(stats);
					}
				}
				// Chunk satisfied — stop without touching the backend.
				if stats.rows >= limit as u64 {
					return Ok(stats);
				}
				// Buffer drained but the chunk wants more — fetch the next page
				// (`fetch_keys_page` advances the resume bound + exhaustion).
				let res = fetch_keys_page(
					self.tx,
					&mut self.rng,
					self.dir,
					self.version,
					&mut self.skip,
					limit,
				)
				.await?;
				if res.keys.is_empty() {
					return Ok(stats);
				}
				self.pending = res.keys.into_iter();
			}
		})
	}
}

/// Default vals-cursor implementation: see [`DefaultKeysCursor`].
pub struct DefaultValsCursor<'a, T: ?Sized> {
	/// The backing transaction. Borrowed for the cursor's lifetime.
	tx: &'a T,
	/// Remaining range to scan. Updated after each batch.
	rng: KeyRange<'a>,
	/// Iteration direction, fixed at open time.
	dir: Direction,
	/// Optional version timestamp for versioned reads.
	version: Option<u64>,
	/// Number of leading items to skip on the first batch.
	skip: u32,
	/// Concatenated key bytes for the most recent batch. Reused.
	key_buf: Vec<u8>,
	/// Concatenated value bytes for the most recent batch. Reused.
	val_buf: Vec<u8>,
	/// One `KeyValSpan` per pair. Reused.
	spans: Vec<KeyValSpan>,
	/// Rows `for_each` fetched into a page but did not visit — kept here so the
	/// next call resumes from the buffer instead of re-scanning them. A page is
	/// only partially drained when a chunk ends mid-page: after an early
	/// `Break`, or when the chunk's `limit` is reached before the buffered page
	/// is exhausted (e.g. a later call with a smaller `limit`).
	///
	/// `next_batch` ignores this buffer (it reads from `rng`), so the buffer
	/// must be empty whenever `next_batch` runs: interleaving the two paths on
	/// one cursor is unsupported and would silently skip these buffered rows.
	/// Debug-asserted at the top of `next_batch`.
	pending: std::vec::IntoIter<(Vec<u8>, Val)>,
}

impl<'a, T: ?Sized> DefaultValsCursor<'a, T> {
	/// Construct a fresh default cursor. The backing key/value buffers
	/// start empty; their capacity grows on the first `next_batch` call
	/// and persists across subsequent batches.
	pub(crate) fn new(
		tx: &'a T,
		rng: KeyRange<'a>,
		dir: Direction,
		version: Option<u64>,
		skip: u32,
	) -> Self {
		Self {
			tx,
			rng,
			dir,
			version,
			skip,
			key_buf: Vec::new(),
			val_buf: Vec::new(),
			spans: Vec::new(),
			pending: Vec::new().into_iter(),
		}
	}
}

impl<T> ScanCursorVals for DefaultValsCursor<'_, T>
where
	T: Transactable + ?Sized,
{
	fn next_batch<'s>(&'s mut self, limit: u32) -> BoxFut<'s, Result<ValsBatch<'s>>> {
		Box::pin(async move {
			// `next_batch` reads from `rng`, not from `for_each`'s `pending`
			// buffer; mixing the two paths on one cursor would silently skip
			// any rows `for_each` left buffered (after an early `Break`, or a
			// chunk that hit its `limit` mid-page). No consumer mixes them —
			// assert the invariant in debug builds.
			debug_assert!(
				self.pending.as_slice().is_empty(),
				"next_batch called while for_each left rows buffered (early Break or mid-page limit stop); the two paths must not be mixed on one cursor",
			);
			// Fetch one page and pack it into the reusable arenas; the
			// successor/exhaustion bookkeeping lives in `fill_vals_batch`.
			let (key_bytes, value_bytes) = fill_vals_batch(
				self.tx,
				&mut self.rng,
				self.dir,
				self.version,
				&mut self.skip,
				&mut self.key_buf,
				&mut self.val_buf,
				&mut self.spans,
				limit,
			)
			.await?;
			// Hand back borrowed slices over the freshly populated arenas
			// alongside the raw byte totals reported by the backend.
			Ok(ValsBatch::from_parts(
				&self.key_buf,
				&self.val_buf,
				&self.spans,
				key_bytes,
				value_bytes,
			))
		})
	}

	/// Drive the visitor over the next chunk without the second copy that
	/// `next_batch` pays: fetch one `scan`/`scanr` page (the backend's single
	/// owned copy) and hand each `(key, value)` straight to `f`, instead of
	/// concatenating the page into the cursor's flat buffers. Backends whose
	/// scan already returns owned data (mem via `Bytes`, surrealkv, tikv,
	/// indxdb) thus drop from two copies to one on the streaming path.
	fn for_each<'s>(
		&'s mut self,
		limit: u32,
		f: &'s mut dyn ValVisitor,
	) -> BoxFut<'s, Result<ScanChunkStats>> {
		Box::pin(async move {
			// Stats accumulate row-by-row (counted only after the visitor
			// accepts each), so an early `Break` reports exactly what was seen.
			let mut stats = ScanChunkStats::default();
			loop {
				// Drain the buffered tail of the previously fetched page first:
				// an earlier `Break` may have left unvisited rows here, which we
				// resume from instead of re-scanning them from the backend.
				while stats.rows < limit as u64 {
					let Some((k, v)) = self.pending.next() else {
						break;
					};
					let flow = f(&k, &v)?;
					stats.rows += 1;
					stats.key_bytes += k.len() as u64;
					stats.value_bytes += v.len() as u64;
					if let std::ops::ControlFlow::Break(()) = flow {
						return Ok(stats);
					}
				}
				// Chunk satisfied — stop without touching the backend.
				if stats.rows >= limit as u64 {
					return Ok(stats);
				}
				// Buffer drained but the chunk wants more — fetch the next page
				// (`fetch_vals_page` advances the resume bound + exhaustion).
				let res = fetch_vals_page(
					self.tx,
					&mut self.rng,
					self.dir,
					self.version,
					&mut self.skip,
					limit,
				)
				.await?;
				if res.values.is_empty() {
					return Ok(stats);
				}
				self.pending = res.values.into_iter();
			}
		})
	}
}
