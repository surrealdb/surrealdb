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

use std::borrow::Cow;
use std::ops::Bound;
use std::slice;

use anyhow::Result;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId};
use crate::expr::BinaryOperator;
use crate::idx::entry::IndexEntryValue;
use crate::idx::keys::{compute_index_range, entry_range};
use crate::key::schema::{DbRoot, EntryFdOpenPrefix, EntryFdPrefix, EntryPrefix, UniqueKey};
use crate::key::{KVKey, KVSubspace, KeyRange, TypedRange};
use crate::kvs::util::{scan, scanr};
use crate::kvs::{Direction, Transaction, Val};
use crate::val::{Array, RecordId, Value};

/// Maximum number of KV entries fetched per batch in index scans.
///
/// A larger value reduces round-trips to the KV store but increases
/// per-batch memory usage.  Range iterators request exactly this many
/// entries; unique-index iterators request `INDEX_BATCH_SIZE + 1` so
/// they can detect exhaustion in a single round-trip.
pub(crate) const INDEX_BATCH_SIZE: u32 = 1000;

/// Decode a batch of KV pairs into [`RecordId`]s.
///
/// The key is ignored; only the value is deserialized: a revision-encoded
/// `RecordId`, optionally followed by an appended doc-ID which
/// `revision::from_slice` ignores (see
/// [`crate::idx::entry::IndexEntryValue`]).  Used by iterators that do not
/// need per-key filtering.
fn decode_record_ids(res: Vec<(Vec<u8>, Val)>) -> Result<Vec<RecordId>> {
	let mut records = Vec::with_capacity(res.len());
	for (_, val) in res {
		let rid: RecordId = revision::from_slice(&val)?;
		records.push(rid);
	}
	Ok(records)
}

/// Decode a batch of KV pairs into entry doc-IDs for bitmap candidate plans.
///
/// Doc-IDs found in the entry values are inserted into `docs`. Entries whose
/// value predates the doc-ID format (see
/// [`crate::idx::entry::IndexEntryValue`]) are pushed onto `missing` so the
/// caller can resolve their doc-ID through the table's shared `!di` mapping.
/// Returns the number of entries decoded.
pub(crate) fn decode_entry_doc_ids(
	res: Vec<(Vec<u8>, Val)>,
	docs: &mut roaring::RoaringTreemap,
	missing: &mut Vec<RecordId>,
) -> Result<usize> {
	use crate::key::KVValue;

	let count = res.len();
	for (_, val) in res {
		let entry = IndexEntryValue::kv_decode_value(&val, ())?;
		match entry.doc_id {
			Some(doc_id) => {
				docs.insert(doc_id);
			}
			None => missing.push(entry.rid),
		}
	}
	Ok(count)
}

/// Iterator for equality lookups on non-unique (`Idx`) indexes.
///
/// Non-unique indexes store one KV entry per (value, record-id) pair, so an
/// equality lookup may match many entries.  This iterator scans the
/// half-open range `[prefix_ids_beg, prefix_ids_end)` in forward or
/// backward order, advancing/retreating the cursor after each batch.
pub(crate) struct IndexEqualIterator {
	/// The entries not yet returned, narrowed after each batch.
	range: TypedRange<IndexEntryValue>,
	/// Whether to scan in reverse (highest to lowest key order).
	reverse: bool,
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
		let range = EntryFdPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(slice::from_ref(value)),
		}
		.range()?;
		Ok(Self {
			range,
			reverse,
		})
	}

	/// Fetch the next batch of matching record IDs.
	///
	/// Returns an empty `Vec` when iteration is complete.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		let res = if self.reverse {
			scanr(&mut self.range, tx, INDEX_BATCH_SIZE).await?
		} else {
			scan(&mut self.range, tx, INDEX_BATCH_SIZE).await?
		};

		decode_record_ids(res)
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
	range: TypedRange<IndexEntryValue>,
}

impl UniqueEqualIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		value: &Value,
	) -> Result<Self> {
		let array = Array::from(vec![value.clone()]);
		let range = if array.is_any_none_or_null() {
			EntryFdPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(&array),
			}
			.range()?
		} else {
			let key = UniqueKey {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(&array),
			}
			.encode_key()?;
			// A unique key is a complete key, so the range covering it alone ends at
			// its immediate successor.
			let end = key.as_borrowed().next();
			entry_range(ns, db, ix, (key..end).into())
		};
		Ok(Self {
			range,
		})
	}

	pub async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		let res = scan(&mut self.range, tx, INDEX_BATCH_SIZE).await?;
		decode_record_ids(res)
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
	range: TypedRange<IndexEntryValue>,
}

impl IndexRangeForwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let range = compute_index_range(ns, db, ix, from, to)?;

		Ok(Self {
			range,
		})
	}

	/// Fetch the next batch of record IDs in ascending key order.
	///
	/// On the first call, if the lower bound is exclusive, any key matching
	/// the original `beg` is skipped.  Subsequent batches need no such
	/// check because `beg` has already been advanced past that key.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		let r = scan(&mut self.range, tx, INDEX_BATCH_SIZE).await?;
		decode_record_ids(r)
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
	range: TypedRange<IndexEntryValue>,
}

impl IndexRangeBackwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let range = compute_index_range(ns, db, ix, from, to)?;

		Ok(Self {
			range,
		})
	}

	/// Fetch the next batch of record IDs in descending key order.
	///
	/// On the first call, if the upper bound is exclusive, keys equal to
	/// `end` are skipped.  On *every* call, if the lower bound is exclusive,
	/// keys equal to the original `beg` are skipped (because `beg` is fixed
	/// and the half-open range always includes it).
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		let r = scanr(&mut self.range, tx, INDEX_BATCH_SIZE).await?;
		decode_record_ids(r)
	}
}

/// Direction-dispatching wrapper for range scans on non-unique indexes.
///
/// Delegates to [`IndexRangeForwardIterator`] or
/// [`IndexRangeBackwardIterator`] depending on the [`Direction`]
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
		from: Bound<&Value>,
		to: Bound<&Value>,
		direction: Direction,
	) -> Result<Self> {
		match direction {
			Direction::Forward => {
				Ok(Self::Forward(IndexRangeForwardIterator::new(ns, db, ix, from, to)?))
			}
			Direction::Backward => {
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

/// Compute the range of entries covered by a unique index range scan.
///
/// Non-nullish values use the exact encoded unique key (no record-ID
/// suffix).  NONE/NULL values are stored with the non-unique key format
/// (record-ID suffix), so we use prefix-based bounds to match them.
fn compute_unique_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	from: Bound<&Value>,
	to: Bound<&Value>,
) -> Result<TypedRange<IndexEntryValue>> {
	let prefix = DbRoot {
		ns,
		db,
	};
	let start = match from {
		Bound::Included(x) => {
			let slice = slice::from_ref(x);
			// These two keys have semantically (when considering the stored index keys) the same
			// meaning when used as a range bound.
			// However, for clearity, and because the end bound do have to be different we do use a
			// different key type to create the bound.
			if x.is_nullish() {
				EntryFdOpenPrefix {
					ns: prefix.ns,
					db: prefix.db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_bound()?
			} else {
				UniqueKey {
					ns: prefix.ns,
					db: prefix.db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_key()?
			}
		}
		Bound::Excluded(x) => {
			let slice = slice::from_ref(x);
			if x.is_nullish() {
				EntryFdOpenPrefix {
					ns: prefix.ns,
					db: prefix.db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.skip_extensions()?
			} else {
				UniqueKey {
					ns: prefix.ns,
					db: prefix.db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_key()?
				.next_neighbour_expect()
			}
		}
		Bound::Unbounded => EntryPrefix {
			ns: prefix.ns,
			db: prefix.db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		}
		.encode_bound()?,
	};

	let end = match to {
		Bound::Included(x) => {
			let slice = slice::from_ref(x);
			if x.is_nullish() {
				EntryFdOpenPrefix {
					ns: prefix.ns,
					db: prefix.db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.skip_extensions()?
			} else {
				UniqueKey {
					ns: prefix.ns,
					db: prefix.db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_key()?
				.next_neighbour_expect()
			}
		}
		Bound::Excluded(x) => {
			let slice = slice::from_ref(x);
			if x.is_nullish() {
				EntryFdOpenPrefix {
					ns: prefix.ns,
					db: prefix.db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_bound()?
			} else {
				UniqueKey {
					ns: prefix.ns,
					db: prefix.db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_key()?
			}
		}
		Bound::Unbounded => EntryPrefix {
			ns: prefix.ns,
			db: prefix.db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		}
		.skip_extensions()?,
	};

	Ok(entry_range(ns, db, ix, (start..end).into()))
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
	range: TypedRange<IndexEntryValue>,
}

impl UniqueRangeForwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let range = compute_unique_range(ns, db, ix, from, to)?;

		Ok(Self {
			range,
		})
	}

	/// Fetch the next batch of record IDs in ascending key order.
	///
	/// On the first call, if the lower bound is exclusive, keys equal to
	/// the original `beg` are skipped.  When the scan is exhausted and
	/// `end_inclusive` is `true`, a final point-get on `end` retrieves the
	/// boundary value that the half-open range excluded.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		let r = scan(&mut self.range, tx, INDEX_BATCH_SIZE).await?;
		decode_record_ids(r)
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
	range: TypedRange<IndexEntryValue>,
}

impl UniqueRangeBackwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let range = compute_unique_range(ns, db, ix, from, to)?;

		Ok(Self {
			range,
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
		let r = scanr(&mut self.range, tx, INDEX_BATCH_SIZE).await?;
		decode_record_ids(r)
	}
}

/// Direction-dispatching wrapper for range scans on unique indexes.
///
/// Delegates to [`UniqueRangeForwardIterator`] or
/// [`UniqueRangeBackwardIterator`] depending on the [`Direction`]
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
		from: Bound<&Value>,
		to: Bound<&Value>,
		direction: Direction,
	) -> Result<Self> {
		match direction {
			Direction::Forward => {
				Ok(Self::Forward(UniqueRangeForwardIterator::new(ns, db, ix, from, to)?))
			}
			Direction::Backward => {
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
/// Supports both forward and backward scanning, controlled by [`Direction`].
/// Forward scans use `tx.scan()` and advance the `beg` cursor;
/// backward scans use `tx.scanr()` and retreat the `end` cursor.
pub(crate) struct CompoundEqualIterator {
	range: TypedRange<IndexEntryValue>,
	/// Scan direction
	direction: Direction,
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
		direction: Direction,
	) -> Result<Self> {
		let range = compute_compound_key_range(ns, db, ix, prefix, range)?;
		Ok(Self {
			range,
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
		let scan_limit = limit.clamp(1, INDEX_BATCH_SIZE);
		let res = match self.direction {
			Direction::Forward => scan(&mut self.range, tx, scan_limit).await?,
			Direction::Backward => scanr(&mut self.range, tx, scan_limit).await?,
		};

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
	range: TypedRange<IndexEntryValue>,
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
		let range = compute_compound_key_range(ns, db, ix, prefix, Some(range))?;
		Ok(Self {
			range,
		})
	}

	/// Fetch the next batch of record IDs in ascending key order,
	/// capped at `limit` entries.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let scan_limit = limit.min(INDEX_BATCH_SIZE);
		let res = scan(&mut self.range, tx, scan_limit).await?;
		decode_record_ids(res)
	}
}

/// Direction-dispatching wrapper for compound range scans.
///
/// Delegates to [`CompoundRangeForwardIterator`] or
/// [`CompoundRangeBackwardIterator`] depending on the [`Direction`]
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
		direction: Direction,
	) -> Result<Self> {
		match direction {
			Direction::Forward => {
				Ok(Self::Forward(CompoundRangeForwardIterator::new(ns, db, ix, prefix, range)?))
			}
			Direction::Backward => {
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
	range: TypedRange<IndexEntryValue>,
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
		let range = compute_compound_key_range(ns, db, ix, prefix, Some(range))?;
		Ok(Self {
			range,
		})
	}

	/// Fetch the next batch of record IDs in descending key order,
	/// capped at `limit` entries.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let scan_limit = limit.min(INDEX_BATCH_SIZE);

		let res = scanr(&mut self.range, tx, scan_limit).await?;

		decode_record_ids(res)
	}
}

/// Compute the KV range covering a B-tree access shape, for consumers that
/// drain raw entries directly (bitmap candidate scans).
///
/// Reuses the same range construction as the streaming iterators above, so a
/// drain sees exactly the entries the equivalent streaming scan would.
/// `FullText`/`Knn` access shapes are not B-tree scans and return an error.
pub(crate) fn bitmap_scan_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	access: &crate::exec::index::access_path::BTreeAccess,
) -> Result<TypedRange<IndexEntryValue>> {
	use crate::exec::index::access_path::BTreeAccess;
	let unique = matches!(ix.index, crate::catalog::Index::Uniq);
	Ok(match access {
		BTreeAccess::Equality(v) => {
			if unique {
				UniqueEqualIterator::new(ns, db, ix, v)?.range
			} else {
				IndexEqualIterator::new(ns, db, ix, v)?.range
			}
		}
		BTreeAccess::Range {
			range,
		} => {
			if unique {
				compute_unique_range(ns, db, ix, range.start.as_ref(), range.end.as_ref())?
			} else {
				compute_index_range(ns, db, ix, range.start.as_ref(), range.end.as_ref())?
			}
		}
		BTreeAccess::Compound {
			prefix,
			range,
		} => compute_compound_key_range(ns, db, ix, prefix, range.as_ref())?,
		BTreeAccess::FullText {
			..
		}
		| BTreeAccess::Knn {
			..
		} => {
			return Err(anyhow::anyhow!("Access shape is not a B-tree scan and has no key range"));
		}
	})
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Compute the range of entries covered by a compound index scan.
///
/// The boundaries come from two bounds over the open field prefix: `prefix`, the
/// equality values of the leading columns, and — where a range condition on the
/// next column is present — `prefix ++ val`, those values followed by the range
/// value.  A boundary is then the bound's own bytes (*at*), the first key
/// strictly beneath it (*beneath*), or the first key after it and everything
/// that extends it (*past*):
///
/// | Operator | start                   | end                  |
/// |----------|-------------------------|----------------------|
/// | `=`      | beneath `prefix ++ val` | past `prefix ++ val` |
/// | `>`      | past `prefix ++ val`    | past `prefix`        |
/// | `>=`     | at `prefix ++ val`      | past `prefix`        |
/// | `<`      | at `prefix`             | at `prefix ++ val`   |
/// | `<=`     | at `prefix`             | past `prefix ++ val` |
///
/// When no range is present, the scan covers everything beneath `prefix`.
fn compute_compound_key_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	prefix: &[Value],
	range: Option<&(BinaryOperator, Value)>,
) -> Result<TypedRange<IndexEntryValue>> {
	let db_prefix = DbRoot {
		ns,
		db,
	};

	// Returns a key which constains the prefix values, without the searched for value
	let without_bound = || {
		EntryFdOpenPrefix {
			ns: db_prefix.ns,
			db: db_prefix.db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(prefix),
		}
		.encode_bound()
	};

	if let Some((op, val)) = range {
		// Returns a key which constains the prefix values, with the searched for value
		let with_bound = || {
			let mut key_values: Vec<Value> = prefix.to_vec();
			key_values.push(val.clone());
			EntryFdOpenPrefix {
				ns: db_prefix.ns,
				db: db_prefix.db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(&key_values),
			}
			.encode_bound()
		};

		let key_range: KeyRange<'static> = match op {
			BinaryOperator::Equal | BinaryOperator::ExactEqual => with_bound()?.prefix_expect(),
			BinaryOperator::MoreThan => {
				let start = with_bound()?.next_neighbour_expect();
				let end = without_bound()?.next_neighbour_expect();

				(start..end).into()
			}
			BinaryOperator::MoreThanEqual => {
				let start = with_bound()?;
				let end = without_bound()?.next_neighbour_expect();
				(start..end).into()
			}
			BinaryOperator::LessThan => {
				let start = without_bound()?;
				let end = with_bound()?;
				(start..end).into()
			}
			BinaryOperator::LessThanEqual => {
				let start = without_bound()?;
				let end = with_bound()?.next_neighbour_expect();
				(start..end).into()
			}
			_ => without_bound()?.prefix_expect(),
		};
		Ok(entry_range(ns, db, ix, key_range))
	} else {
		Ok(entry_range(ns, db, ix, without_bound()?.prefix_expect()))
	}
}

#[cfg(test)]
mod tests {
	//! B-tree iterator behaviour against real, committed index data.
	//!
	//! Each fixture defines its index and writes its rows as separate
	//! statements, then opens a read context: an index defined in the same
	//! transaction as the `CREATE`s is never backfilled, so a scan over it
	//! would be silently empty. Every test therefore asserts a non-empty
	//! result before asserting anything finer.
	//!
	//! Record ids are compared as SurrealQL text (`t:a`) so a failure names
	//! the rows rather than a byte range.

	use std::sync::Arc;

	use surrealdb_types::ToSql;

	use super::*;
	use crate::exec::ExecutionContext;
	use crate::exec::operators::test_util::TestDb;
	use crate::val::Number;

	/// One bound-combination case: a label for failure messages, the lower and
	/// upper bounds to scan, and the record ids expected in iteration order.
	type BoundCase<'a> = (&'a str, Bound<&'a Value>, Bound<&'a Value>, Vec<&'a str>);

	/// A committed table plus everything its iterators need: the namespace and
	/// database ids, the online index definitions, and a read transaction.
	///
	/// The [`ExecutionContext`] is kept because the transaction lives exactly as
	/// long as the context that opened it.
	struct Fixture {
		ctx: ExecutionContext,
		ns: NamespaceId,
		db: DatabaseId,
		indexes: Arc<[IndexDefinition]>,
	}

	impl Fixture {
		/// Open a read context over `table` and resolve its online indexes.
		async fn new(db: &TestDb, table: &str) -> Self {
			let ctx = db.exec_ctx().await;
			let dbc = ctx.database().expect("database-level context").clone();
			let tb: surrealdb_strand::TableName = table.into();
			let indexes = dbc.get_table_indexes(&tb, None).await.expect("index definitions");
			assert!(!indexes.is_empty(), "table {table} has no online index to scan");
			Self {
				ns: dbc.ns().namespace_id,
				db: dbc.db.database_id,
				indexes,
				ctx,
			}
		}

		fn ix(&self, name: &str) -> &IndexDefinition {
			self.indexes
				.iter()
				.find(|i| i.name.as_str() == name)
				.unwrap_or_else(|| panic!("index {name} is not online"))
		}

		fn tx(&self) -> Arc<Transaction> {
			self.ctx.database().expect("database-level context").txn()
		}
	}

	/// Drain an iterator whose `next_batch` takes only the transaction.
	///
	/// Returns the record ids in emission order plus the per-batch sizes, so a
	/// test can assert both the result and how it was chunked. An empty batch
	/// is the documented end-of-iteration signal.
	macro_rules! drain {
		($it:expr, $tx:expr) => {{
			let mut ids: Vec<String> = Vec::new();
			let mut sizes: Vec<usize> = Vec::new();
			loop {
				let batch = $it.next_batch(&$tx).await.expect("batch should scan");
				if batch.is_empty() {
					break;
				}
				sizes.push(batch.len());
				ids.extend(batch.iter().map(|r| r.to_sql()));
			}
			(ids, sizes)
		}};
	}

	/// As [`drain`], for the compound iterators, whose `next_batch` takes a
	/// caller-supplied per-batch entry cap.
	macro_rules! drain_capped {
		($it:expr, $tx:expr, $limit:expr) => {{
			let mut ids: Vec<String> = Vec::new();
			let mut sizes: Vec<usize> = Vec::new();
			loop {
				let batch = $it.next_batch(&$tx, $limit).await.expect("batch should scan");
				if batch.is_empty() {
					break;
				}
				sizes.push(batch.len());
				ids.extend(batch.iter().map(|r| r.to_sql()));
			}
			(ids, sizes)
		}};
	}

	fn reversed(ids: &[&str]) -> Vec<String> {
		ids.iter().rev().map(|s| (*s).to_owned()).collect()
	}

	fn owned(ids: &[&str]) -> Vec<String> {
		ids.iter().map(|s| (*s).to_owned()).collect()
	}

	// ------------------------------------------------------------------
	// Non-unique (`Idx`) index
	// ------------------------------------------------------------------

	/// `t` with a non-unique index on `v`: two rows share `v = 1`, and one row
	/// each holds NONE and NULL.
	async fn idx_db() -> TestDb {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; DEFINE INDEX iv ON t FIELDS v;").await;
		db.run(
			"CREATE t:a SET v = 1;
			 CREATE t:b SET v = 1;
			 CREATE t:c SET v = 2;
			 CREATE t:d SET v = 3;
			 CREATE t:e SET v = NONE;
			 CREATE t:f SET v = NULL;",
		)
		.await;
		db
	}

	#[tokio::test]
	async fn equality_returns_every_duplicate_in_record_order() {
		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let mut it = IndexEqualIterator::new(fx.ns, fx.db, fx.ix("iv"), &Value::from(1i64))
			.expect("equality iterator");
		let (ids, _) = drain!(it, tx);
		// A non-unique index stores one entry per (value, record-id) pair, so
		// both rows on `v = 1` come back, ordered by the record-id suffix.
		assert_eq!(ids, owned(&["t:a", "t:b"]));
	}

	#[tokio::test]
	async fn equality_backward_reverses_the_record_order() {
		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let mut it =
			IndexEqualIterator::with_direction(fx.ns, fx.db, fx.ix("iv"), &Value::from(1i64), true)
				.expect("reverse equality iterator");
		let (ids, _) = drain!(it, tx);
		assert_eq!(ids, reversed(&["t:a", "t:b"]));
	}

	#[tokio::test]
	async fn equality_on_an_absent_value_yields_nothing() {
		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let mut it = IndexEqualIterator::new(fx.ns, fx.db, fx.ix("iv"), &Value::from(99i64))
			.expect("equality iterator");
		let (ids, sizes) = drain!(it, tx);
		assert!(ids.is_empty(), "no row holds v = 99, got {ids:?}");
		assert!(sizes.is_empty(), "an exhausted scan yields no batch at all");
	}

	#[tokio::test]
	async fn none_and_null_are_separately_indexed_keys() {
		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();

		let mut it = IndexEqualIterator::new(fx.ns, fx.db, fx.ix("iv"), &Value::None)
			.expect("equality iterator");
		let (none_ids, _) = drain!(it, tx);
		assert_eq!(none_ids, owned(&["t:e"]));

		let mut it = IndexEqualIterator::new(fx.ns, fx.db, fx.ix("iv"), &Value::Null)
			.expect("equality iterator");
		let (null_ids, _) = drain!(it, tx);
		assert_eq!(null_ids, owned(&["t:f"]), "NULL is a distinct key from NONE");
	}

	#[tokio::test]
	async fn unbounded_range_covers_every_row_in_index_order() {
		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();

		// Key order puts the nullish values below the numbers.
		let expected = ["t:e", "t:f", "t:a", "t:b", "t:c", "t:d"];

		let mut it = IndexRangeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iv"),
			Bound::Unbounded,
			Bound::Unbounded,
			Direction::Forward,
		)
		.expect("range iterator");
		let (forward, _) = drain!(it, tx);
		assert_eq!(forward, owned(&expected));

		let mut it = IndexRangeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iv"),
			Bound::Unbounded,
			Bound::Unbounded,
			Direction::Backward,
		)
		.expect("range iterator");
		let (backward, _) = drain!(it, tx);
		assert_eq!(backward, reversed(&expected), "a backward scan is the exact reverse");
	}

	#[tokio::test]
	async fn range_bound_combinations_include_the_documented_edges() {
		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let (v1, v2, v3, v4, v9) = (
			Value::from(1i64),
			Value::from(2i64),
			Value::from(3i64),
			Value::from(4i64),
			Value::from(9i64),
		);

		// An inclusive bound admits every entry for that value; an exclusive
		// bound admits none of them.
		let cases: Vec<BoundCase<'_>> = vec![
			("[1,2]", Bound::Included(&v1), Bound::Included(&v2), vec!["t:a", "t:b", "t:c"]),
			("(1,3]", Bound::Excluded(&v1), Bound::Included(&v3), vec!["t:c", "t:d"]),
			("[1,3)", Bound::Included(&v1), Bound::Excluded(&v3), vec!["t:a", "t:b", "t:c"]),
			("(1,3)", Bound::Excluded(&v1), Bound::Excluded(&v3), vec!["t:c"]),
			("..1)", Bound::Unbounded, Bound::Excluded(&v1), vec!["t:e", "t:f"]),
			("[2..", Bound::Included(&v2), Bound::Unbounded, vec!["t:c", "t:d"]),
			("(3..", Bound::Excluded(&v3), Bound::Unbounded, vec![]),
			("[4,9]", Bound::Included(&v4), Bound::Included(&v9), vec![]),
		];

		for (label, from, to, expected) in cases {
			let mut it =
				IndexRangeIterator::new(fx.ns, fx.db, fx.ix("iv"), from, to, Direction::Forward)
					.expect("range iterator");
			let (forward, _) = drain!(it, tx);
			assert_eq!(forward, owned(&expected), "forward {label}");

			let mut it =
				IndexRangeIterator::new(fx.ns, fx.db, fx.ix("iv"), from, to, Direction::Backward)
					.expect("range iterator");
			let (backward, _) = drain!(it, tx);
			assert_eq!(backward, reversed(&expected), "backward {label}");
		}
	}

	#[tokio::test]
	async fn contradictory_bounds_scan_nothing_rather_than_failing() {
		// The analyser folds a contradiction into `AccessPath::EmptyScan`, so
		// the iterator never sees one in a planned query; it must still be safe
		// to construct and drain.
		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let v3 = Value::from(3i64);
		let mut it = IndexRangeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iv"),
			Bound::Excluded(&v3),
			Bound::Excluded(&v3),
			Direction::Forward,
		)
		.expect("range iterator");
		let (ids, _) = drain!(it, tx);
		assert!(ids.is_empty(), "an empty range yields no rows, got {ids:?}");
	}

	#[tokio::test]
	async fn equality_resumes_across_batch_boundaries() {
		// One value shared by more rows than a single batch holds, so the
		// cursor has to advance (forward) and retreat (backward) past the
		// last-returned key without dropping or repeating an entry.
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS; DEFINE INDEX iv ON t FIELDS v;").await;
		let total = INDEX_BATCH_SIZE as usize + 100;
		let rows =
			(0..total).map(|i| format!("{{ id: {i}, v: 1 }}")).collect::<Vec<_>>().join(", ");
		db.run(&format!("INSERT INTO t [{rows}];")).await;

		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let value = Value::from(1i64);

		let mut it =
			IndexEqualIterator::new(fx.ns, fx.db, fx.ix("iv"), &value).expect("equality iterator");
		let (forward, sizes) = drain!(it, tx);
		assert_eq!(sizes, vec![INDEX_BATCH_SIZE as usize, 100], "a batch caps at INDEX_BATCH_SIZE");
		assert_eq!(forward.len(), total);
		let unique: std::collections::HashSet<&String> = forward.iter().collect();
		assert_eq!(unique.len(), total, "no entry is returned twice across batches");
		assert_eq!(forward.first().map(String::as_str), Some("t:0"));
		assert_eq!(forward.last(), Some(&format!("t:{}", total - 1)));

		let mut it = IndexEqualIterator::with_direction(fx.ns, fx.db, fx.ix("iv"), &value, true)
			.expect("reverse equality iterator");
		let (backward, sizes) = drain!(it, tx);
		assert_eq!(sizes, vec![INDEX_BATCH_SIZE as usize, 100]);
		let mut expected = forward.clone();
		expected.reverse();
		assert_eq!(backward, expected, "the backward scan reverses the forward one exactly");
	}

	// ------------------------------------------------------------------
	// Unique (`Uniq`) index
	// ------------------------------------------------------------------

	/// `u` with a unique index on `k`, plus two rows whose `k` is NONE — a
	/// unique index stores nullish tuples in the non-unique key format, so they
	/// do not collide.
	async fn uniq_db() -> TestDb {
		let db =
			TestDb::new("DEFINE TABLE u SCHEMALESS; DEFINE INDEX ik ON u FIELDS k UNIQUE;").await;
		db.run(
			"CREATE u:1 SET k = 10;
			 CREATE u:2 SET k = 20;
			 CREATE u:3 SET k = 30;
			 CREATE u:4 SET k = NONE;
			 CREATE u:5 SET k = NONE;",
		)
		.await;
		db
	}

	#[tokio::test]
	async fn unique_equality_is_a_point_lookup() {
		let db = uniq_db().await;
		let fx = Fixture::new(&db, "u").await;
		let tx = fx.tx();

		let mut it = UniqueEqualIterator::new(fx.ns, fx.db, fx.ix("ik"), &Value::from(20i64))
			.expect("unique equality iterator");
		let (ids, sizes) = drain!(it, tx);
		assert_eq!(ids, owned(&["u:2"]));
		assert_eq!(sizes, vec![1], "a unique key resolves in one batch");

		let mut it = UniqueEqualIterator::new(fx.ns, fx.db, fx.ix("ik"), &Value::from(21i64))
			.expect("unique equality iterator");
		let (ids, _) = drain!(it, tx);
		assert!(ids.is_empty(), "no row holds k = 21, got {ids:?}");
	}

	#[tokio::test]
	async fn unique_nullish_equality_matches_every_nullish_row() {
		// NONE/NULL tuples carry a record-id suffix, so one unique index can
		// hold many of them and the lookup must be a prefix scan, not a get.
		let db = uniq_db().await;
		let fx = Fixture::new(&db, "u").await;
		let tx = fx.tx();
		let mut it = UniqueEqualIterator::new(fx.ns, fx.db, fx.ix("ik"), &Value::None)
			.expect("unique equality iterator");
		let (ids, _) = drain!(it, tx);
		assert_eq!(ids, owned(&["u:4", "u:5"]));
	}

	#[tokio::test]
	async fn unique_range_bound_combinations_include_the_documented_edges() {
		let db = uniq_db().await;
		let fx = Fixture::new(&db, "u").await;
		let tx = fx.tx();
		let (k10, k20, k30, k40) =
			(Value::from(10i64), Value::from(20i64), Value::from(30i64), Value::from(40i64));

		let cases: Vec<BoundCase<'_>> = vec![
			// An inclusive upper bound must include the boundary key even
			// though the underlying KV range is half-open.
			("[10,30]", Bound::Included(&k10), Bound::Included(&k30), vec!["u:1", "u:2", "u:3"]),
			("(10,30)", Bound::Excluded(&k10), Bound::Excluded(&k30), vec!["u:2"]),
			("[20..", Bound::Included(&k20), Bound::Unbounded, vec!["u:2", "u:3"]),
			// Nullish entries sort below the numbers.
			("..20)", Bound::Unbounded, Bound::Excluded(&k20), vec!["u:4", "u:5", "u:1"]),
			("..", Bound::Unbounded, Bound::Unbounded, vec!["u:4", "u:5", "u:1", "u:2", "u:3"]),
			("[40..", Bound::Included(&k40), Bound::Unbounded, vec![]),
		];

		for (label, from, to, expected) in cases {
			let mut it =
				UniqueRangeIterator::new(fx.ns, fx.db, fx.ix("ik"), from, to, Direction::Forward)
					.expect("unique range iterator");
			let (forward, _) = drain!(it, tx);
			assert_eq!(forward, owned(&expected), "forward {label}");

			let mut it =
				UniqueRangeIterator::new(fx.ns, fx.db, fx.ix("ik"), from, to, Direction::Backward)
					.expect("unique range iterator");
			let (backward, _) = drain!(it, tx);
			assert_eq!(backward, reversed(&expected), "backward {label}");
		}
	}

	// ------------------------------------------------------------------
	// Compound (multi-column) index
	// ------------------------------------------------------------------

	/// `c` with a compound index on `(a, b)`; `c:4` sits under a different
	/// leading value so a prefix scan must not reach it.
	async fn compound_db() -> TestDb {
		let db = TestDb::new("DEFINE TABLE c SCHEMALESS; DEFINE INDEX iab ON c FIELDS a, b;").await;
		db.run(
			"CREATE c:1 SET a = 1, b = 1;
			 CREATE c:2 SET a = 1, b = 2;
			 CREATE c:3 SET a = 1, b = 3;
			 CREATE c:4 SET a = 2, b = 1;",
		)
		.await;
		db
	}

	#[tokio::test]
	async fn compound_prefix_scan_stays_within_the_prefix() {
		let db = compound_db().await;
		let fx = Fixture::new(&db, "c").await;
		let tx = fx.tx();
		let prefix = vec![Value::from(1i64)];

		let mut it = CompoundEqualIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iab"),
			&prefix,
			None,
			Direction::Forward,
		)
		.expect("compound equality iterator");
		let (forward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		// Ordered by the trailing column, and `c:4` (a = 2) is out of range.
		assert_eq!(forward, owned(&["c:1", "c:2", "c:3"]));

		let mut it = CompoundEqualIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iab"),
			&prefix,
			None,
			Direction::Backward,
		)
		.expect("compound equality iterator");
		let (backward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		assert_eq!(backward, reversed(&["c:1", "c:2", "c:3"]));
	}

	#[tokio::test]
	async fn compound_equality_range_pins_the_full_composite_key() {
		let db = compound_db().await;
		let fx = Fixture::new(&db, "c").await;
		let tx = fx.tx();
		let prefix = vec![Value::from(1i64)];
		let range = (BinaryOperator::Equal, Value::from(2i64));
		let mut it = CompoundEqualIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iab"),
			&prefix,
			Some(&range),
			Direction::Forward,
		)
		.expect("compound equality iterator");
		let (ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		assert_eq!(ids, owned(&["c:2"]), "the range value extends the prefix into an exact key");
	}

	#[tokio::test]
	async fn compound_range_operators_match_the_documented_key_bounds() {
		let db = compound_db().await;
		let fx = Fixture::new(&db, "c").await;
		let tx = fx.tx();
		let prefix = vec![Value::from(1i64)];

		let cases: Vec<(BinaryOperator, Vec<&str>)> = vec![
			(BinaryOperator::MoreThan, vec!["c:3"]),
			(BinaryOperator::MoreThanEqual, vec!["c:2", "c:3"]),
			(BinaryOperator::LessThan, vec!["c:1"]),
			(BinaryOperator::LessThanEqual, vec!["c:1", "c:2"]),
			(BinaryOperator::Equal, vec!["c:2"]),
		];

		for (op, expected) in cases {
			let range = (op.clone(), Value::from(2i64));
			let mut it = CompoundRangeIterator::new(
				fx.ns,
				fx.db,
				fx.ix("iab"),
				&prefix,
				&range,
				Direction::Forward,
			)
			.expect("compound range iterator");
			let (forward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
			// Every bound is anchored on the prefix, so `c:4` (a = 2) can
			// never leak in whichever way the trailing column is bounded.
			assert_eq!(forward, owned(&expected), "forward b {op:?} 2");

			let mut it = CompoundRangeIterator::new(
				fx.ns,
				fx.db,
				fx.ix("iab"),
				&prefix,
				&range,
				Direction::Backward,
			)
			.expect("compound range iterator");
			let (backward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
			assert_eq!(backward, reversed(&expected), "backward b {op:?} 2");
		}
	}

	#[tokio::test]
	async fn unsupported_range_operator_widens_to_the_whole_prefix() {
		// Any operator outside the documented table degrades to the prefix
		// range: an over-approximation the residual filter has to narrow.
		let db = compound_db().await;
		let fx = Fixture::new(&db, "c").await;
		let tx = fx.tx();
		let prefix = vec![Value::from(1i64)];
		let range = (BinaryOperator::NotEqual, Value::from(2i64));
		let mut it = CompoundRangeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iab"),
			&prefix,
			&range,
			Direction::Forward,
		)
		.expect("compound range iterator");
		let (ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		assert_eq!(ids, owned(&["c:1", "c:2", "c:3"]), "c:2 is not excluded by the key range");
	}

	#[tokio::test]
	async fn compound_scan_resumes_across_capped_batches() {
		let db = compound_db().await;
		let fx = Fixture::new(&db, "c").await;
		let tx = fx.tx();
		let prefix = vec![Value::from(1i64)];

		let mut it = CompoundEqualIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iab"),
			&prefix,
			None,
			Direction::Forward,
		)
		.expect("compound equality iterator");
		let (forward, sizes) = drain_capped!(it, tx, 1);
		assert_eq!(sizes, vec![1, 1, 1], "the caller's cap bounds every batch");
		assert_eq!(forward, owned(&["c:1", "c:2", "c:3"]));

		let mut it = CompoundEqualIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iab"),
			&prefix,
			None,
			Direction::Backward,
		)
		.expect("compound equality iterator");
		let (backward, sizes) = drain_capped!(it, tx, 1);
		assert_eq!(sizes, vec![1, 1, 1]);
		assert_eq!(backward, reversed(&["c:1", "c:2", "c:3"]));
	}

	#[tokio::test]
	async fn zero_cap_reads_one_entry_on_equal_and_none_on_range() {
		// `CompoundEqualIterator` clamps the cap up to 1 while
		// `CompoundRangeForwardIterator` passes 0 through, where a 0-entry read
		// is indistinguishable from exhaustion. Neither is the query's LIMIT:
		// the scan pipeline tracks that itself and truncates the batch.
		let db = compound_db().await;
		let fx = Fixture::new(&db, "c").await;
		let tx = fx.tx();
		let prefix = vec![Value::from(1i64)];

		let mut it = CompoundEqualIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iab"),
			&prefix,
			None,
			Direction::Forward,
		)
		.expect("compound equality iterator");
		let batch = it.next_batch(&tx, 0).await.expect("batch should scan");
		assert_eq!(batch.len(), 1);

		let range = (BinaryOperator::MoreThanEqual, Value::from(1i64));
		let mut it = CompoundRangeForwardIterator::new(fx.ns, fx.db, fx.ix("iab"), &prefix, &range)
			.expect("compound range iterator");
		let batch = it.next_batch(&tx, 0).await.expect("batch should scan");
		assert!(batch.is_empty());
	}

	// ------------------------------------------------------------------
	// Bitmap candidate ranges
	// ------------------------------------------------------------------

	#[tokio::test]
	async fn bitmap_range_drains_the_same_entries_as_the_streaming_scan() {
		use crate::exec::index::access_path::BTreeAccess;

		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let ix = fx.ix("iv");

		let mut it = IndexEqualIterator::new(fx.ns, fx.db, ix, &Value::from(1i64))
			.expect("equality iterator");
		let (streamed, _) = drain!(it, tx);
		assert_eq!(streamed.len(), 2, "the fixture has two rows on v = 1");

		let access = BTreeAccess::Equality(Value::from(1i64));
		let mut range = bitmap_scan_range(fx.ns, fx.db, ix, &access).expect("b-tree range");
		let entries = scan(&mut range, &tx, INDEX_BATCH_SIZE).await.expect("drain");
		let mut docs = roaring::RoaringTreemap::new();
		let mut missing = Vec::new();
		let decoded = decode_entry_doc_ids(entries, &mut docs, &mut missing).expect("decode");
		assert_eq!(decoded, streamed.len(), "the drain sees exactly the streamed entries");
		assert!(missing.is_empty(), "a current-format index carries a doc-ID on every entry");
		assert_eq!(docs.len() as usize, streamed.len());
	}

	#[tokio::test]
	async fn bitmap_range_rejects_non_btree_access_shapes() {
		use crate::exec::index::access_path::BTreeAccess;
		use crate::expr::operator::{BooleanOperator, MatchesOperator};

		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let ix = fx.ix("iv");

		let full_text = BTreeAccess::FullText {
			query: "hello".to_owned(),
			operator: MatchesOperator {
				rf: None,
				operator: BooleanOperator::And,
			},
		};
		assert!(bitmap_scan_range(fx.ns, fx.db, ix, &full_text).is_err());

		let knn = BTreeAccess::Knn {
			vector: vec![Number::Int(1)],
			k: 3,
			ef: 10,
		};
		assert!(bitmap_scan_range(fx.ns, fx.db, ix, &knn).is_err());
	}
}
