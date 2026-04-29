//! B-tree index iterators for `Idx` (non-unique) and `Uniq` (unique) indexes.
//!
//! These iterators provide efficient, batched record retrieval using B-tree
//! index structures.  They support:
//!
//! - **Equality lookups** – [`IndexEqualIterator`] / [`UniqueEqualIterator`]
//! - **Range scans** – [`IndexRangeIterator`] / [`UniqueRangeIterator`]
//! - **Compound prefix scans** – [`CompoundEqualIterator`] / [`CompoundRangeForwardIterator`]
//!
//! ### Batching strategy
//!
//! All iterators produce records in batches of up to [`INDEX_BATCH_SIZE`].
//! After each batch the iterator advances (or retreats, for backward scans)
//! a cursor so the next call resumes where the previous one left off.
//!
//! ### KV range convention
//!
//! The underlying KV store uses **half-open** ranges `[beg, end)`.  This
//! means:
//! - `beg` is *included* in the scan result.
//! - `end` is *excluded* from the scan result.
//!
//! For **forward** scans (`tx.scan`), each batch advances `beg` past the
//! last returned key (by appending `0x00`).  For **backward** scans
//! (`tx.scanr`), each batch retreats `end` to the last returned key
//! (which is then excluded from the next batch by the half-open semantics).
//!
//! ### Exclusive boundary handling
//!
//! When a query boundary is *exclusive* (e.g. `v > 5`), the computed key
//! may still fall inside the half-open range.  The iterators handle this
//! with post-scan filtering:
//!
//! - **Leading-edge** boundary (the first key that might appear): filtered on the *first* batch
//!   only, then the flag is cleared.
//! - **Trailing-edge** boundary (the last key that might appear, relevant for backward scans where
//!   `beg` stays fixed): filtered on *every* batch because the cursor never moves past it.

use anyhow::Result;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId};
use crate::exec::index::access_path::RangeBound;
use crate::expr::BinaryOperator;
use crate::idx::planner::ScanDirection;
use crate::key::index::Index;
use crate::kvs::{KVKey, Key, Transaction, Val};
use crate::val::{Array, RecordId, Value};

/// Maximum number of KV entries fetched per batch in index scans.
///
/// A larger value reduces round-trips to the KV store but increases
/// per-batch memory usage.  Range iterators request exactly this many
/// entries; unique-index iterators request `INDEX_BATCH_SIZE + 1` so
/// they can detect exhaustion in a single round-trip.
const INDEX_BATCH_SIZE: u32 = 1000;

/// Decode a batch of KV pairs into [`RecordId`]s.
///
/// The key is ignored; only the value (a revision-encoded `RecordId`) is
/// deserialized.  Used by iterators that do not need per-key filtering.
fn decode_record_ids(res: Vec<(Key, Val)>) -> Result<Vec<RecordId>> {
	let mut records = Vec::with_capacity(res.len());
	for (_, val) in res {
		let rid: RecordId = revision::from_slice(&val)?;
		records.push(rid);
	}
	Ok(records)
}

/// Iterator for equality lookups on non-unique (`Idx`) indexes.
///
/// Non-unique indexes store one KV entry per (value, record-id) pair, so an
/// equality lookup may match many entries.  This iterator scans the
/// half-open range `[prefix_ids_beg, prefix_ids_end)` in forward or
/// backward order, advancing/retreating the cursor after each batch.
pub(crate) struct IndexEqualIterator {
	/// Lower bound of the remaining scan range (inclusive).
	beg: Vec<u8>,
	/// Upper bound of the scan range (exclusive, fixed).
	end: Vec<u8>,
	/// Whether to scan in reverse (highest to lowest key order).
	reverse: bool,
	/// `true` once the scan range is exhausted.
	done: bool,
}

impl IndexEqualIterator {
	/// Create a new equality iterator for the given index value (always forward).
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		value: &Value,
	) -> Result<Self> {
		Self::with_direction(ns, db, ix, value, false)
	}

	/// Create a new equality iterator with explicit direction.
	///
	/// When `reverse` is true, the iterator uses `tx.scanr()` to return
	/// records in descending key order (highest to lowest record ID).
	pub(crate) fn with_direction(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		value: &Value,
		reverse: bool,
	) -> Result<Self> {
		let array = Array::from(vec![value.clone()]);
		let beg = Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?;
		let end = Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?;
		Ok(Self {
			beg,
			end,
			reverse,
			done: false,
		})
	}

	/// Fetch the next batch of matching record IDs.
	///
	/// Returns an empty `Vec` when iteration is complete.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		if self.reverse {
			self.next_batch_reverse(tx).await
		} else {
			self.next_batch_forward(tx).await
		}
	}

	/// Forward scan: iterate from beg to end in ascending key order.
	async fn next_batch_forward(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		let res = tx.scan(self.beg.clone()..self.end.clone(), INDEX_BATCH_SIZE, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Advance `beg` past the last returned key so the next batch
		// starts immediately after it.  Appending 0x00 ensures the key
		// is strictly greater than the last returned key.
		if let Some((key, _)) = res.last() {
			self.beg.clone_from(key);
			self.beg.push(0x00);
		}

		decode_record_ids(res)
	}

	/// Reverse scan: iterate from end to beg in descending key order.
	async fn next_batch_reverse(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		let res = tx.scanr(self.beg.clone()..self.end.clone(), INDEX_BATCH_SIZE, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// scanr returns [highest_key, ..., lowest_key].
		// Update end key for next batch to the lowest key (last in result).
		if let Some((key, _)) = res.last() {
			self.end.clone_from(key);
		}

		// Decode record IDs from values
		let mut records = Vec::with_capacity(res.len());
		for (_, val) in res {
			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		Ok(records)
	}
}

/// Iterator for equality lookups on unique (`Uniq`) indexes.
///
/// Equality lookup on a unique index.
///
/// For non-nullish values this is a single point-get (one KV entry per
/// value).  NONE/NULL tuples are stored with the non-unique key format
/// (record-ID suffix) so they require a prefix range scan instead.
pub(crate) struct UniqueEqualIterator {
	inner: UniqueEqualInner,
}

enum UniqueEqualInner {
	PointGet(Option<Key>),
	PrefixScan {
		beg: Key,
		end: Key,
		done: bool,
	},
}

impl UniqueEqualIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		value: &Value,
	) -> Result<Self> {
		let array = Array::from(vec![value.clone()]);
		let inner = if array.is_any_none_or_null() {
			let beg = Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?;
			let end = Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?;
			UniqueEqualInner::PrefixScan {
				beg,
				end,
				done: false,
			}
		} else {
			let key = Index::new(ns, db, &ix.table_name, ix.index_id, &array, None).encode_key()?;
			UniqueEqualInner::PointGet(Some(key))
		};
		Ok(Self {
			inner,
		})
	}

	pub async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		match &mut self.inner {
			UniqueEqualInner::PointGet(key) => {
				let Some(key) = key.take() else {
					return Ok(Vec::new());
				};
				if let Some(val) = tx.get(&key, None).await? {
					let rid: RecordId = revision::from_slice(&val)?;
					Ok(vec![rid])
				} else {
					Ok(Vec::new())
				}
			}
			UniqueEqualInner::PrefixScan {
				beg,
				end,
				done,
			} => {
				if *done {
					return Ok(Vec::new());
				}
				let res = tx.scan(beg.clone()..end.clone(), INDEX_BATCH_SIZE, 0, None).await?;
				if res.is_empty() {
					*done = true;
					return Ok(Vec::new());
				}
				if let Some((key, _)) = res.last() {
					beg.clone_from(key);
					beg.push(0x00);
				}
				decode_record_ids(res)
			}
		}
	}
}

/// Compute the begin key for a non-unique index range scan.
///
/// Returns `(key, inclusive)` where:
/// - **inclusive bound** (`>=`): uses `prefix_ids_beg` so the scan starts at the first entry for
///   the given value.
/// - **exclusive bound** (`>`): uses `prefix_ids_end` so the scan starts *after* all entries for
///   the given value.
/// - **no bound**: uses the index-wide `prefix_beg` (start of index).
fn compute_index_range_beg_key(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	from: Option<&RangeBound>,
) -> Result<(Key, bool)> {
	if let Some(from) = from {
		let array = Array::from(vec![from.value.clone()]);
		if from.inclusive {
			Ok((Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?, true))
		} else {
			Ok((Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?, false))
		}
	} else {
		Ok((Index::prefix_beg(ns, db, &ix.table_name, ix.index_id)?, true))
	}
}

/// Compute the end key for a non-unique index range scan.
///
/// Returns `(key, inclusive)` where:
/// - **inclusive bound** (`<=`): uses `prefix_ids_end` so the scan covers all entries for the given
///   value.
/// - **exclusive bound** (`<`): uses `prefix_ids_beg` so the scan stops *before* any entry for the
///   given value.
/// - **no bound**: uses the index-wide `prefix_end` (end of index).
fn compute_index_range_end_key(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	to: Option<&RangeBound>,
) -> Result<(Key, bool)> {
	if let Some(to) = to {
		let array = Array::from(vec![to.value.clone()]);
		if to.inclusive {
			Ok((Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?, true))
		} else {
			Ok((Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?, false))
		}
	} else {
		Ok((Index::prefix_end(ns, db, &ix.table_name, ix.index_id)?, true))
	}
}

/// Forward iterator for range scans on non-unique (`Idx`) indexes.
///
/// Scans `[beg, end)` using `tx.scan()`, advancing the `beg` cursor after
/// each batch.  When the lower bound is *exclusive*, the first batch
/// filters out keys equal to the original `beg` (the "leading-edge" key).
/// Once that first batch is processed, `beg_checked` is set to `true` and
/// no further filtering is needed because `beg` has already been advanced
/// past the excluded key.
pub(crate) struct IndexRangeForwardIterator {
	/// Lower bound of the remaining scan range (advances after each batch).
	beg: Key,
	/// Upper bound of the scan range (fixed).
	end: Key,
	/// `true` once the leading-edge exclusive boundary has been handled.
	/// Initialised to `true` when the lower bound is inclusive (no
	/// filtering required).
	beg_checked: bool,
	/// `true` once the scan range is exhausted.
	done: bool,
}

impl IndexRangeForwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Option<&RangeBound>,
		to: Option<&RangeBound>,
	) -> Result<Self> {
		let (beg, beg_inclusive) = compute_index_range_beg_key(ns, db, ix, from)?;
		let (end, _end_inclusive) = compute_index_range_end_key(ns, db, ix, to)?;

		Ok(Self {
			beg,
			end,
			beg_checked: beg_inclusive,
			done: false,
		})
	}

	/// Fetch the next batch of record IDs in ascending key order.
	///
	/// On the first call, if the lower bound is exclusive, any key matching
	/// the original `beg` is skipped.  Subsequent batches need no such
	/// check because `beg` has already been advanced past that key.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		// Capture the key to exclude *before* we advance the cursor.
		let check_exclusive_beg = if self.beg_checked {
			None
		} else {
			Some(self.beg.clone())
		};

		let res = tx.scan(self.beg.clone()..self.end.clone(), INDEX_BATCH_SIZE, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Advance `beg` past the last returned key for the next batch.
		if let Some((key, _)) = res.last() {
			self.beg.clone_from(key);
			self.beg.push(0x00);
		}

		self.beg_checked = true;

		let mut records = Vec::with_capacity(res.len());
		for (key, val) in res {
			// Skip the excluded leading-edge key (first batch only).
			if let Some(ref exclusive_key) = check_exclusive_beg
				&& key == *exclusive_key
			{
				continue;
			}
			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		Ok(records)
	}
}

/// Backward iterator for range scans on non-unique (`Idx`) indexes.
///
/// Scans `[beg, end)` using `tx.scanr()`, retreating the `end` cursor
/// after each batch.  Two kinds of exclusive-boundary filtering apply:
///
/// 1. **Leading-edge (`end`)**: When the upper bound is exclusive, the first batch filters out keys
///    equal to `end`.  After that batch `end` is retreated, so the excluded key can never reappear.
///    `end_checked` tracks whether this has been done.
///
/// 2. **Trailing-edge (`beg`)**: When the lower bound is exclusive, `beg` remains fixed throughout
///    iteration (only `end` moves).  Therefore the excluded key can appear in *any* batch and must
///    be filtered on *every* call.  `exclude_beg_key` holds the key to filter.
pub(crate) struct IndexRangeBackwardIterator {
	/// Lower bound of the scan range (fixed; only `end` moves).
	beg: Key,
	/// Upper bound of the remaining scan range (retreats after each batch).
	end: Key,
	/// `true` once the leading-edge exclusive `end` boundary has been
	/// handled.  Initialised to `true` when the upper bound is inclusive.
	end_checked: bool,
	/// Key to exclude at the `beg` (trailing) edge.  `Some` when the
	/// lower bound is exclusive; checked on every batch.
	exclude_beg_key: Option<Key>,
	/// `true` once the scan range is exhausted.
	done: bool,
}

impl IndexRangeBackwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Option<&RangeBound>,
		to: Option<&RangeBound>,
	) -> Result<Self> {
		let (beg, beg_inclusive) = compute_index_range_beg_key(ns, db, ix, from)?;
		let (end, end_inclusive) = compute_index_range_end_key(ns, db, ix, to)?;

		let exclude_beg_key = if beg_inclusive {
			None
		} else {
			Some(beg.clone())
		};

		Ok(Self {
			beg,
			end,
			end_checked: end_inclusive,
			exclude_beg_key,
			done: false,
		})
	}

	/// Fetch the next batch of record IDs in descending key order.
	///
	/// On the first call, if the upper bound is exclusive, keys equal to
	/// `end` are skipped.  On *every* call, if the lower bound is exclusive,
	/// keys equal to the original `beg` are skipped (because `beg` is fixed
	/// and the half-open range always includes it).
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		// Capture the key to exclude *before* we retreat the cursor.
		let check_exclusive_end = if self.end_checked {
			None
		} else {
			Some(self.end.clone())
		};

		let res = tx.scanr(self.beg.clone()..self.end.clone(), INDEX_BATCH_SIZE, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Retreat `end` to the last returned key.  The half-open range
		// `[beg, end)` will then exclude this key on the next batch,
		// preventing duplicates.
		if let Some((key, _)) = res.last() {
			self.end.clone_from(key);
		}

		self.end_checked = true;

		let mut records = Vec::with_capacity(res.len());
		for (key, val) in res {
			// Skip the excluded leading-edge key (first batch only).
			if let Some(ref exclusive_key) = check_exclusive_end
				&& key == *exclusive_key
			{
				continue;
			}
			// Skip the excluded trailing-edge key (every batch).
			if let Some(ref beg_key) = self.exclude_beg_key
				&& key == *beg_key
			{
				continue;
			}
			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		Ok(records)
	}
}

/// Direction-dispatching wrapper for range scans on non-unique indexes.
///
/// Delegates to [`IndexRangeForwardIterator`] or
/// [`IndexRangeBackwardIterator`] depending on the [`ScanDirection`]
/// provided at construction time.
pub(crate) enum IndexRangeIterator {
	Forward(IndexRangeForwardIterator),
	Backward(IndexRangeBackwardIterator),
}

impl IndexRangeIterator {
	/// Create a new range iterator for the given direction.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Option<&RangeBound>,
		to: Option<&RangeBound>,
		direction: ScanDirection,
	) -> Result<Self> {
		match direction {
			ScanDirection::Forward => {
				Ok(Self::Forward(IndexRangeForwardIterator::new(ns, db, ix, from, to)?))
			}
			ScanDirection::Backward => {
				Ok(Self::Backward(IndexRangeBackwardIterator::new(ns, db, ix, from, to)?))
			}
		}
	}

	/// Fetch the next batch, delegating to the inner iterator.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		match self {
			Self::Forward(iter) => iter.next_batch(tx).await,
			Self::Backward(iter) => iter.next_batch(tx).await,
		}
	}
}

// ---------------------------------------------------------------------------
// Unique-index range helpers
// ---------------------------------------------------------------------------

/// Compute the begin key for a unique index range scan.
///
/// Non-nullish values use the exact encoded unique key (no record-ID
/// suffix).  NONE/NULL values are stored with the non-unique key format
/// (record-ID suffix), so we use prefix-based bounds to match them.
fn compute_unique_range_beg_key(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	from: Option<&RangeBound>,
) -> Result<(Key, bool)> {
	if let Some(from) = from {
		let array = Array::from(vec![from.value.clone()]);
		if array.is_any_none_or_null() {
			let key = if from.inclusive {
				Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?
			} else {
				Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?
			};
			Ok((key, true))
		} else {
			let key = Index::new(ns, db, &ix.table_name, ix.index_id, &array, None).encode_key()?;
			Ok((key, from.inclusive))
		}
	} else {
		Ok((Index::prefix_beg(ns, db, &ix.table_name, ix.index_id)?, true))
	}
}

/// Compute the end key for a unique index range scan.
///
/// See [`compute_unique_range_beg_key`] for the rationale.
fn compute_unique_range_end_key(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	to: Option<&RangeBound>,
) -> Result<(Key, bool)> {
	if let Some(to) = to {
		let array = Array::from(vec![to.value.clone()]);
		if array.is_any_none_or_null() {
			let key = if to.inclusive {
				Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &array)?
			} else {
				Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &array)?
			};
			// Sentinel boundary keys never store a row; avoid a trailing `get(end)`.
			Ok((key, false))
		} else {
			let key = Index::new(ns, db, &ix.table_name, ix.index_id, &array, None).encode_key()?;
			Ok((key, to.inclusive))
		}
	} else {
		// Sentinel boundary key — no real record is stored here.
		Ok((Index::prefix_end(ns, db, &ix.table_name, ix.index_id)?, false))
	}
}

/// Forward iterator for range scans on unique (`Uniq`) indexes.
///
/// Works similarly to [`IndexRangeForwardIterator`] but operates on unique
/// indexes where each value maps to a single key.  The scan uses
/// `tx.scan()` with an over-sized limit (`INDEX_BATCH_SIZE + 1`) and
/// advances `beg` after each batch.
///
/// Because the half-open range `[beg, end)` inherently *excludes* `end`,
/// an **inclusive** upper bound needs special treatment: when the scan is
/// exhausted (empty result), a final `tx.get(end)` is issued to retrieve
/// the boundary value that the half-open range missed.
pub(crate) struct UniqueRangeForwardIterator {
	/// Lower bound of the remaining scan range (advances after each batch).
	beg: Key,
	/// Upper bound of the scan range (fixed).
	end: Key,
	/// `true` once the leading-edge exclusive `beg` boundary has been
	/// handled.  Initialised to `true` when the lower bound is inclusive.
	beg_checked: bool,
	/// `true` when the upper bound is inclusive and a trailing `get(end)`
	/// should be attempted once the scan is exhausted.
	end_inclusive: bool,
	/// `true` once the scan range is exhausted.
	done: bool,
}

impl UniqueRangeForwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Option<&RangeBound>,
		to: Option<&RangeBound>,
	) -> Result<Self> {
		let (beg, beg_inclusive) = compute_unique_range_beg_key(ns, db, ix, from)?;
		let (end, end_inclusive) = compute_unique_range_end_key(ns, db, ix, to)?;

		Ok(Self {
			beg,
			end,
			beg_checked: beg_inclusive,
			end_inclusive,
			done: false,
		})
	}

	/// Fetch the next batch of record IDs in ascending key order.
	///
	/// On the first call, if the lower bound is exclusive, keys equal to
	/// the original `beg` are skipped.  When the scan is exhausted and
	/// `end_inclusive` is `true`, a final point-get on `end` retrieves the
	/// boundary value that the half-open range excluded.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		// Capture the key to exclude *before* we advance the cursor.
		let check_exclusive_beg = if self.beg_checked {
			None
		} else {
			Some(self.beg.clone())
		};

		let limit = INDEX_BATCH_SIZE + 1;
		let res = tx.scan(self.beg.clone()..self.end.clone(), limit, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			// Inclusive upper bound: the half-open range excluded `end`,
			// so try a direct point-get to include it.
			if self.end_inclusive
				&& let Some(val) = tx.get(&self.end, None).await?
			{
				let rid: RecordId = revision::from_slice(&val)?;
				return Ok(vec![rid]);
			}
			return Ok(Vec::new());
		}

		// Advance `beg` past the last returned key for the next batch.
		if let Some((key, _)) = res.last() {
			self.beg.clone_from(key);
			self.beg.push(0x00);
		}

		self.beg_checked = true;

		let mut records = Vec::with_capacity(res.len());
		for (key, val) in res {
			// Skip the excluded leading-edge key (first batch only).
			if let Some(ref exclusive_key) = check_exclusive_beg
				&& key == *exclusive_key
			{
				continue;
			}
			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		Ok(records)
	}
}

/// Backward iterator for range scans on unique (`Uniq`) indexes.
///
/// Works similarly to [`IndexRangeBackwardIterator`] but for unique indexes.
/// Uses `tx.scanr()` and retreats the `end` cursor after each batch.
///
/// Exclusive boundary handling follows the same leading-edge / trailing-edge
/// pattern described on [`IndexRangeBackwardIterator`]:
/// - `end_checked` guards the first-batch-only filter for an exclusive `end`.
/// - `exclude_beg_key` is checked on every batch for an exclusive `beg`.
pub(crate) struct UniqueRangeBackwardIterator {
	/// Lower bound of the scan range (fixed; only `end` moves).
	beg: Key,
	/// Upper bound of the remaining scan range (retreats after each batch).
	end: Key,
	/// `true` once the leading-edge exclusive `end` boundary has been
	/// handled.  Initialised to `true` when the upper bound is inclusive.
	end_checked: bool,
	/// When `true`, the first `next_batch` call issues a point-get on
	/// `original_end` to retrieve the inclusive upper-bound record that
	/// the half-open `scanr` excludes (symmetric with the forward
	/// iterator's trailing-get in `UniqueRangeForwardIterator`).
	end_inclusive: bool,
	/// The original end key before any cursor retreat.  Needed for the
	/// inclusive upper-bound point-get.
	original_end: Key,
	/// Key to exclude at the `beg` (trailing) edge.  `Some` when the
	/// lower bound is exclusive; checked on every batch.
	exclude_beg_key: Option<Key>,
	/// `true` once the scan range is exhausted.
	done: bool,
}

impl UniqueRangeBackwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Option<&RangeBound>,
		to: Option<&RangeBound>,
	) -> Result<Self> {
		let (beg, beg_inclusive) = compute_unique_range_beg_key(ns, db, ix, from)?;
		let (end, end_inclusive) = compute_unique_range_end_key(ns, db, ix, to)?;

		let exclude_beg_key = if beg_inclusive {
			None
		} else {
			Some(beg.clone())
		};

		Ok(Self {
			beg,
			original_end: end.clone(),
			end,
			end_checked: end_inclusive,
			end_inclusive,
			exclude_beg_key,
			done: false,
		})
	}

	/// Fetch the next batch of record IDs in descending key order.
	///
	/// On the first call, if the upper bound is inclusive, a point-get on
	/// the original end key retrieves the boundary record that `scanr`'s
	/// half-open range excludes.  If the upper bound is exclusive, keys
	/// equal to `end` are skipped.  On *every* call, if the lower bound
	/// is exclusive, keys equal to `beg` are skipped.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		let mut records = Vec::new();

		// Inclusive upper bound: the half-open scan excludes `end`, so
		// retrieve it via point-get.  In DESC order the boundary key is
		// the largest and appears first.
		if self.end_inclusive {
			self.end_inclusive = false;
			if let Some(val) = tx.get(&self.original_end, None).await? {
				let rid: RecordId = revision::from_slice(&val)?;
				records.push(rid);
			}
		}

		// Capture the key to exclude *before* we retreat the cursor.
		let check_exclusive_end = if self.end_checked {
			None
		} else {
			Some(self.end.clone())
		};

		let limit = INDEX_BATCH_SIZE + 1;
		let res = tx.scanr(self.beg.clone()..self.end.clone(), limit, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			if records.is_empty() {
				return Ok(Vec::new());
			}
			return Ok(records);
		}

		// Retreat `end` to the last returned key.  The half-open range
		// `[beg, end)` will then exclude this key on the next batch.
		if let Some((key, _)) = res.last() {
			self.end.clone_from(key);
		}

		self.end_checked = true;

		records.reserve(res.len());
		for (key, val) in res {
			// Skip the excluded leading-edge key (first batch only).
			if let Some(ref exclusive_key) = check_exclusive_end
				&& key == *exclusive_key
			{
				continue;
			}
			// Skip the excluded trailing-edge key (every batch).
			if let Some(ref beg_key) = self.exclude_beg_key
				&& key == *beg_key
			{
				continue;
			}
			let rid: RecordId = revision::from_slice(&val)?;
			records.push(rid);
		}

		Ok(records)
	}
}

/// Direction-dispatching wrapper for range scans on unique indexes.
///
/// Delegates to [`UniqueRangeForwardIterator`] or
/// [`UniqueRangeBackwardIterator`] depending on the [`ScanDirection`]
/// provided at construction time.
pub(crate) enum UniqueRangeIterator {
	Forward(UniqueRangeForwardIterator),
	Backward(UniqueRangeBackwardIterator),
}

impl UniqueRangeIterator {
	/// Create a new unique range iterator for the given direction.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Option<&RangeBound>,
		to: Option<&RangeBound>,
		direction: ScanDirection,
	) -> Result<Self> {
		match direction {
			ScanDirection::Forward => {
				Ok(Self::Forward(UniqueRangeForwardIterator::new(ns, db, ix, from, to)?))
			}
			ScanDirection::Backward => {
				Ok(Self::Backward(UniqueRangeBackwardIterator::new(ns, db, ix, from, to)?))
			}
		}
	}

	/// Fetch the next batch, delegating to the inner iterator.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		match self {
			Self::Forward(iter) => iter.next_batch(tx).await,
			Self::Backward(iter) => iter.next_batch(tx).await,
		}
	}
}

// ---------------------------------------------------------------------------
// Compound-index iterators
// ---------------------------------------------------------------------------

/// Iterator for compound (multi-column) index equality scans.
///
/// Supports both forward and backward scanning, controlled by [`ScanDirection`].
/// Forward scans use `tx.scan()` and advance the `beg` cursor;
/// backward scans use `tx.scanr()` and retreat the `end` cursor.
pub(crate) struct CompoundEqualIterator {
	/// Current scan position (begin key)
	beg: Vec<u8>,
	/// End key (exclusive)
	end: Vec<u8>,
	/// Whether iteration is complete
	done: bool,
	/// Scan direction
	direction: ScanDirection,
}

impl CompoundEqualIterator {
	/// Create a new compound equality iterator.
	///
	/// `prefix` contains the fixed equality values for leading columns.
	/// When an additional equality range is present, it is appended to the
	/// prefix so the scan covers the exact composite key.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		range: Option<&(BinaryOperator, Value)>,
		direction: ScanDirection,
	) -> Result<Self> {
		let (beg, end) = compute_compound_key_range(ns, db, ix, prefix, range)?;
		Ok(Self {
			beg,
			end,
			done: false,
			direction,
		})
	}

	/// Fetch the next batch of record IDs, capped at `limit`.
	///
	/// The caller supplies a `limit` so that storage-level scans can be
	/// bounded (e.g. when a pushed-down LIMIT is active).  Pass
	/// `INDEX_BATCH_SIZE` when no external limit applies.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		let scan_limit = limit.min(INDEX_BATCH_SIZE);
		let res = match self.direction {
			ScanDirection::Forward => {
				tx.scan(self.beg.clone()..self.end.clone(), scan_limit, 0, None).await?
			}
			ScanDirection::Backward => {
				tx.scanr(self.beg.clone()..self.end.clone(), scan_limit, 0, None).await?
			}
		};

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Update cursor for next batch
		if let Some((key, _)) = res.last() {
			match self.direction {
				ScanDirection::Forward => {
					self.beg.clone_from(key);
					self.beg.push(0x00);
				}
				ScanDirection::Backward => {
					self.end.clone_from(key);
				}
			}
		}

		decode_record_ids(res)
	}
}

/// Forward iterator for compound (multi-column) index range scans.
///
/// Handles the case where leading columns are fixed by equality and the
/// next column has a range condition (e.g. `WHERE a = 1 AND b > 5`).
/// The key boundaries are computed by [`compute_compound_key_range`],
/// which encodes the equality prefix together with the range value.
pub(crate) struct CompoundRangeForwardIterator {
	/// Lower bound of the remaining scan range (advances after each batch).
	beg: Vec<u8>,
	/// Upper bound of the scan range (exclusive, fixed).
	end: Vec<u8>,
	/// `true` once the scan range is exhausted.
	done: bool,
}

impl CompoundRangeForwardIterator {
	/// Create a new compound range iterator.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		range: &(BinaryOperator, Value),
	) -> Result<Self> {
		let (beg, end) = compute_compound_key_range(ns, db, ix, prefix, Some(range))?;
		Ok(Self {
			beg,
			end,
			done: false,
		})
	}

	/// Fetch the next batch of record IDs in ascending key order,
	/// capped at `limit` entries.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		let scan_limit = limit.min(INDEX_BATCH_SIZE);
		let res = tx.scan(self.beg.clone()..self.end.clone(), scan_limit, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Advance `beg` past the last returned key for the next batch.
		if let Some((key, _)) = res.last() {
			self.beg.clone_from(key);
			self.beg.push(0x00);
		}

		decode_record_ids(res)
	}
}

/// Direction-dispatching wrapper for compound range scans.
///
/// Delegates to [`CompoundRangeForwardIterator`] or
/// [`CompoundRangeBackwardIterator`] depending on the [`ScanDirection`]
/// provided at construction time.
pub(crate) enum CompoundRangeIterator {
	Forward(CompoundRangeForwardIterator),
	Backward(CompoundRangeBackwardIterator),
}

impl CompoundRangeIterator {
	/// Create a new compound range iterator for the given direction.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		range: &(BinaryOperator, Value),
		direction: ScanDirection,
	) -> Result<Self> {
		match direction {
			ScanDirection::Forward => {
				Ok(Self::Forward(CompoundRangeForwardIterator::new(ns, db, ix, prefix, range)?))
			}
			ScanDirection::Backward => {
				Ok(Self::Backward(CompoundRangeBackwardIterator::new(ns, db, ix, prefix, range)?))
			}
		}
	}

	/// Fetch the next batch, delegating to the inner iterator.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		match self {
			Self::Forward(iter) => iter.next_batch(tx, limit).await,
			Self::Backward(iter) => iter.next_batch(tx, limit).await,
		}
	}
}

/// Backward iterator for compound (multi-column) index range scans.
///
/// Mirrors [`CompoundRangeForwardIterator`] but scans in descending key
/// order using `tx.scanr()`.  The `end` cursor retreats after each batch
/// while `beg` stays fixed, following the same pattern as
/// [`IndexRangeBackwardIterator`].
pub(crate) struct CompoundRangeBackwardIterator {
	/// Lower bound of the scan range (fixed; only `end` moves).
	beg: Vec<u8>,
	/// Upper bound of the remaining scan range (retreats after each batch).
	end: Vec<u8>,
	/// `true` once the scan range is exhausted.
	done: bool,
}

impl CompoundRangeBackwardIterator {
	/// Create a new backward compound range iterator.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		range: &(BinaryOperator, Value),
	) -> Result<Self> {
		let (beg, end) = compute_compound_key_range(ns, db, ix, prefix, Some(range))?;
		Ok(Self {
			beg,
			end,
			done: false,
		})
	}

	/// Fetch the next batch of record IDs in descending key order,
	/// capped at `limit` entries.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		if self.done {
			return Ok(Vec::new());
		}

		let scan_limit = limit.min(INDEX_BATCH_SIZE);
		let res = tx.scanr(self.beg.clone()..self.end.clone(), scan_limit, 0, None).await?;

		if res.is_empty() {
			self.done = true;
			return Ok(Vec::new());
		}

		// Retreat `end` to the last returned key.  The half-open range
		// `[beg, end)` will then exclude this key on the next batch,
		// preventing duplicates.
		if let Some((key, _)) = res.last() {
			self.end.clone_from(key);
		}

		decode_record_ids(res)
	}
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Compute the KV key range `(beg, end)` for a compound index scan.
///
/// Builds the appropriate prefix-based key boundaries depending on whether
/// the scan is a pure equality prefix or has a range condition on the
/// next column.
///
/// For range conditions, the operator determines which `Index::prefix_ids_*`
/// helper is used:
///
/// | Operator | `beg`                  | `end`                       |
/// |----------|------------------------|-----------------------------|
/// | `=`      | `prefix_ids_composite_beg(val)` | `prefix_ids_composite_end(val)` |
/// | `>`      | `prefix_ids_end(val)`  | `prefix_ids_composite_end(prefix)` |
/// | `>=`     | `prefix_ids_beg(val)`  | `prefix_ids_composite_end(prefix)` |
/// | `<`      | `prefix_ids_composite_beg(prefix)` | `prefix_ids_beg(val)` |
/// | `<=`     | `prefix_ids_composite_beg(prefix)` | `prefix_ids_end(val)` |
///
/// When no range is present, the scan covers the full composite prefix.
fn compute_compound_key_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	prefix: &[Value],
	range: Option<&(BinaryOperator, Value)>,
) -> Result<(Vec<u8>, Vec<u8>)> {
	let prefix_array = Array::from(prefix.to_vec());

	if let Some((op, val)) = range {
		let mut key_values: Vec<Value> = prefix.to_vec();
		key_values.push(val.clone());
		let key_array = Array::from(key_values);

		match op {
			BinaryOperator::Equal | BinaryOperator::ExactEqual => {
				let beg = Index::prefix_ids_composite_beg(
					ns,
					db,
					&ix.table_name,
					ix.index_id,
					&key_array,
				)?;
				let end = Index::prefix_ids_composite_end(
					ns,
					db,
					&ix.table_name,
					ix.index_id,
					&key_array,
				)?;
				Ok((beg, end))
			}
			BinaryOperator::MoreThan => {
				let beg = Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &key_array)?;
				let end = Index::prefix_ids_composite_end(
					ns,
					db,
					&ix.table_name,
					ix.index_id,
					&prefix_array,
				)?;
				Ok((beg, end))
			}
			BinaryOperator::MoreThanEqual => {
				let beg = Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &key_array)?;
				let end = Index::prefix_ids_composite_end(
					ns,
					db,
					&ix.table_name,
					ix.index_id,
					&prefix_array,
				)?;
				Ok((beg, end))
			}
			BinaryOperator::LessThan => {
				let beg = Index::prefix_ids_composite_beg(
					ns,
					db,
					&ix.table_name,
					ix.index_id,
					&prefix_array,
				)?;
				let end = Index::prefix_ids_beg(ns, db, &ix.table_name, ix.index_id, &key_array)?;
				Ok((beg, end))
			}
			BinaryOperator::LessThanEqual => {
				let beg = Index::prefix_ids_composite_beg(
					ns,
					db,
					&ix.table_name,
					ix.index_id,
					&prefix_array,
				)?;
				let end = Index::prefix_ids_end(ns, db, &ix.table_name, ix.index_id, &key_array)?;
				Ok((beg, end))
			}
			_ => {
				let beg = Index::prefix_ids_composite_beg(
					ns,
					db,
					&ix.table_name,
					ix.index_id,
					&prefix_array,
				)?;
				let end = Index::prefix_ids_composite_end(
					ns,
					db,
					&ix.table_name,
					ix.index_id,
					&prefix_array,
				)?;
				Ok((beg, end))
			}
		}
	} else {
		let beg =
			Index::prefix_ids_composite_beg(ns, db, &ix.table_name, ix.index_id, &prefix_array)?;
		let end =
			Index::prefix_ids_composite_end(ns, db, &ix.table_name, ix.index_id, &prefix_array)?;
		Ok((beg, end))
	}
}
