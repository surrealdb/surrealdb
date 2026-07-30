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
use crate::idx::keys::compute_index_range;
use crate::idx::planner::ScanDirection;
use crate::key::database::all::DatabaseRoot;
use crate::key::index::{IndexPrefix, IndexPrefixTerminated, IndexPrefixUnterminated, UniqueIndex};
use crate::key::{KVKey, KVRange, KeyRange};
use crate::kvs::util::{scan, scanr};
use crate::kvs::{Transaction, Val};
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
/// [`crate::key::index::IndexEntryValue`]).  Used by iterators that do not
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
/// [`crate::key::index::IndexEntryValue`]) are pushed onto `missing` so the
/// caller can resolve their doc-ID through the table's shared `!di` mapping.
/// Returns the number of entries decoded.
pub(crate) fn decode_entry_doc_ids(
	res: Vec<(Vec<u8>, Val)>,
	docs: &mut roaring::RoaringTreemap,
	missing: &mut Vec<RecordId>,
) -> Result<usize> {
	use crate::key::KVValue;
	use crate::key::index::IndexEntryValue;

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
	/// Lower bound of the remaining scan range (inclusive).
	range: KeyRange<'static>,
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
		let range = IndexPrefixTerminated {
			prefix: DatabaseRoot {
				ns,
				db,
			},
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(slice::from_ref(value)),
		}
		.encode_range()?;
		Ok(Self {
			range,
			reverse,
		})
	}

	/// Fetch the next batch of matching record IDs.
	///
	/// Returns an empty `Vec` when iteration is complete.
	pub(crate) async fn next_batch(&mut self, tx: &Transaction) -> Result<Vec<RecordId>> {
		if self.range.is_empty() {
			return Ok(Vec::new());
		}

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
	range: KeyRange<'static>,
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
			IndexPrefixTerminated {
				prefix: DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(&array),
			}
			.encode_range()?
		} else {
			let key = UniqueIndex {
				prefix: DatabaseRoot {
					ns,
					db,
				},
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(&array),
			}
			.encode_key()?;
			let end = key.as_borrowed().next();
			(key..end).into()
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
	range: KeyRange<'static>,
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
	range: KeyRange<'static>,
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
		from: Bound<&Value>,
		to: Bound<&Value>,
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
fn compute_unique_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	from: Bound<&Value>,
	to: Bound<&Value>,
) -> Result<KeyRange<'static>> {
	let prefix = DatabaseRoot {
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
				IndexPrefixUnterminated {
					prefix,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_bound()?
			} else {
				UniqueIndex {
					prefix,
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
				IndexPrefixUnterminated {
					prefix,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_bound()?
				.next_neighbour_expect()
			} else {
				UniqueIndex {
					prefix,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_key()?
				.next_neighbour_expect()
			}
		}
		Bound::Unbounded => IndexPrefix {
			prefix,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		}
		.encode_bound()?,
	};

	let end = match to {
		Bound::Included(x) => {
			let slice = slice::from_ref(x);
			if x.is_nullish() {
				IndexPrefixUnterminated {
					prefix,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_bound()?
				.next_neighbour_expect()
			} else {
				UniqueIndex {
					prefix,
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
				IndexPrefixUnterminated {
					prefix,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_bound()?
			} else {
				UniqueIndex {
					prefix,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(slice),
				}
				.encode_key()?
			}
		}
		Bound::Unbounded => IndexPrefix {
			prefix,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		}
		.encode_bound()?
		.next_neighbour_expect(),
	};

	Ok((start..end).into())
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
	range: KeyRange<'static>,
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
	range: KeyRange<'static>,
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
		from: Bound<&Value>,
		to: Bound<&Value>,
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
	range: KeyRange<'static>,
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
			ScanDirection::Forward => scan(&mut self.range, tx, scan_limit).await?,
			ScanDirection::Backward => scanr(&mut self.range, tx, scan_limit).await?,
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
	range: KeyRange<'static>,
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
	range: KeyRange<'static>,
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
) -> Result<KeyRange<'static>> {
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
) -> Result<KeyRange<'static>> {
	let db_prefix = DatabaseRoot {
		ns,
		db,
	};

	// Returns a key which constains the prefix values, without the searched for value
	let without_bound = || {
		IndexPrefixUnterminated {
			prefix: db_prefix,
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
			IndexPrefixUnterminated {
				prefix: db_prefix,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(&key_values),
			}
			.encode_bound()
		};

		match op {
			BinaryOperator::Equal | BinaryOperator::ExactEqual => Ok(with_bound()?.prefix_expect()),
			BinaryOperator::MoreThan => {
				let start = with_bound()?.next_neighbour_expect();
				let end = without_bound()?.next_neighbour_expect();

				Ok((start..end).into())
			}
			BinaryOperator::MoreThanEqual => {
				let start = with_bound()?;
				let end = without_bound()?.next_neighbour_expect();
				Ok((start..end).into())
			}
			BinaryOperator::LessThan => {
				let start = without_bound()?;
				let end = with_bound()?;
				Ok((start..end).into())
			}
			BinaryOperator::LessThanEqual => {
				let start = without_bound()?;
				let end = with_bound()?.next_neighbour_expect();
				Ok((start..end).into())
			}
			_ => Ok(without_bound()?.prefix_expect()),
		}
	} else {
		Ok(without_bound()?.prefix_expect())
	}
}
