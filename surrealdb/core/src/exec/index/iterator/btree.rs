//! B-tree index iterators for `Idx` (non-unique) and `Uniq` (unique) indexes.
//!
//! These iterators return record ids in batches, so a large index range is
//! never loaded at once. The range helpers return the raw KV range for the
//! bitmap candidate scans, which drain the entries themselves. They support:
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

#[cfg(test)]
use std::ops::Bound;

use anyhow::Result;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId};
#[cfg(test)]
use crate::exec::index::access_path::BTreeAccess;
#[cfg(test)]
use crate::expr::BinaryOperator;
use crate::idx::entry::IndexEntryValue;
use crate::idx::kernels::btree::IndexKeyspace;
#[cfg(test)]
use crate::idx::kernels::btree::decode_record_id;
use crate::key::TypedRange;
use crate::kvs::Val;
#[cfg(test)]
use crate::kvs::util::{scan, scanr};
#[cfg(test)]
use crate::kvs::{Direction, Transaction};
use crate::val::RecordId;
#[cfg(test)]
use crate::val::Value;

/// Maximum number of KV entries fetched per batch in index scans.
///
/// A larger value reduces round-trips to the KV store but increases
/// per-batch memory usage.  Range iterators request exactly this many
/// entries; unique-index iterators request `INDEX_BATCH_SIZE + 1` so
/// they can detect exhaustion in a single round-trip.
pub(crate) const INDEX_BATCH_SIZE: u32 = 1000;

/// Decode a batch of KV pairs into [`RecordId`]s.
///
/// The key is ignored; each value is decoded by the entry kernel
/// ([`decode_record_id`]), which reads the record id an entry of either
/// format names.
#[cfg(test)]
fn decode_record_ids(res: Vec<(Vec<u8>, Val)>) -> Result<Vec<RecordId>> {
	let mut records = Vec::with_capacity(res.len());
	for (_, val) in res {
		records.push(decode_record_id(&val)?);
	}
	Ok(records)
}

/// Decode a batch of KV pairs into entry doc-IDs for bitmap candidate plans.
///
/// Doc-IDs found in the entry values are inserted into `docs`. Entries whose
/// value carries no doc-ID (see
/// [`crate::idx::entry::IndexEntryValue`]) are pushed onto `missing` so the
/// caller can resolve their doc-ID through the table's shared `!dj` mapping.
/// Returns the number of entries decoded.
pub(crate) fn decode_entry_doc_ids(
	res: Vec<(Vec<u8>, Val)>,
	docs: &mut roaring::RoaringTreemap,
	missing: &mut Vec<RecordId>,
	spatial: bool,
	point_filter: Option<&crate::exec::index::access_path::SpatialPointFilter>,
) -> Result<usize> {
	use crate::key::KVValue;

	let count = res.len();
	if let Some(pf) = point_filter {
		for (_, val) in res {
			let (doc_id, point) = IndexEntryValue::decode_spatial_trailer(&val)?;
			let (Some(doc_id), Some((x, y))) = (doc_id, point) else {
				anyhow::bail!(
					"Spatial point filter applied to index entry without embedded doc-ID and point coordinates"
				);
			};
			if pf.matches(x, y) {
				docs.insert(doc_id);
			}
		}
		return Ok(count);
	}
	for (_, val) in res {
		let fast_doc_id = if spatial {
			IndexEntryValue::decode_spatial_trailer(&val).map(|(doc_id, _)| doc_id)
		} else {
			IndexEntryValue::decode_trailer(&val)
		};
		if let Ok(Some(doc_id)) = fast_doc_id {
			docs.insert(doc_id);
			continue;
		}
		let entry = if spatial {
			IndexEntryValue::decode_spatial(&val)?
		} else {
			IndexEntryValue::kv_decode_value(&val, ())?
		};
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
#[cfg(test)]
pub(crate) struct IndexEqualIterator {
	/// The entries not yet returned, narrowed after each batch.
	range: TypedRange<IndexEntryValue>,
	/// Whether to scan in reverse (highest to lowest key order).
	reverse: bool,
}

#[cfg(test)]
impl IndexEqualIterator {
	/// Create a new equality iterator for the given index value (always forward).
	#[cfg(test)]
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
		let range = IndexKeyspace::of(ns, db, ix).equality(false, value)?;
		Ok(Self {
			range,
			reverse,
		})
	}

	/// Fetch the next batch of matching record IDs.
	///
	/// Returns an empty `Vec` when iteration is complete.
	///
	/// Returns at most `limit` record ids, clamped to `1..=INDEX_BATCH_SIZE`.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let res = if self.reverse {
			scanr(&mut self.range, tx, limit.clamp(1, INDEX_BATCH_SIZE)).await?
		} else {
			scan(&mut self.range, tx, limit.clamp(1, INDEX_BATCH_SIZE)).await?
		};

		decode_record_ids(res)
	}
}

/// Iterator for equality lookups on unique (`Uniq`) indexes.
///
/// For a value that takes part in uniqueness this is a single point-get (one
/// KV entry per value). NONE, and NULL unless the index is
/// `NULLS NOT DISTINCT`, are stored with the non-unique key format
/// (record-ID suffix) so they require a prefix range scan instead.
#[cfg(test)]
pub(crate) struct UniqueEqualIterator {
	range: TypedRange<IndexEntryValue>,
}

#[cfg(test)]
impl UniqueEqualIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		value: &Value,
	) -> Result<Self> {
		let range = IndexKeyspace::of(ns, db, ix).equality(true, value)?;
		Ok(Self {
			range,
		})
	}

	/// The next at most `limit` matching record ids, clamped to
	/// `1..=INDEX_BATCH_SIZE`: a NONE/NULL lookup can match many records.
	pub async fn next_batch(&mut self, tx: &Transaction, limit: u32) -> Result<Vec<RecordId>> {
		let res = scan(&mut self.range, tx, limit.clamp(1, INDEX_BATCH_SIZE)).await?;
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
#[cfg(test)]
pub(crate) struct IndexRangeForwardIterator {
	range: TypedRange<IndexEntryValue>,
}

#[cfg(test)]
impl IndexRangeForwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let range = IndexKeyspace::of(ns, db, ix).range(from, to)?;

		Ok(Self {
			range,
		})
	}

	/// Fetch the next batch of record IDs in ascending key order.
	///
	/// On the first call, if the lower bound is exclusive, any key matching
	/// the original `beg` is skipped.  Subsequent batches need no such
	/// check because `beg` has already been advanced past that key.
	///
	/// Returns at most `limit` record ids, clamped to `1..=INDEX_BATCH_SIZE`.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let r = scan(&mut self.range, tx, limit.clamp(1, INDEX_BATCH_SIZE)).await?;
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
#[cfg(test)]
pub(crate) struct IndexRangeBackwardIterator {
	range: TypedRange<IndexEntryValue>,
}

#[cfg(test)]
impl IndexRangeBackwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let range = IndexKeyspace::of(ns, db, ix).range(from, to)?;

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
	///
	/// Returns at most `limit` record ids, clamped to `1..=INDEX_BATCH_SIZE`.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let r = scanr(&mut self.range, tx, limit.clamp(1, INDEX_BATCH_SIZE)).await?;
		decode_record_ids(r)
	}
}

/// Direction-dispatching wrapper for range scans on non-unique indexes.
///
/// Delegates to [`IndexRangeForwardIterator`] or
/// [`IndexRangeBackwardIterator`] depending on the [`Direction`]
/// provided at construction time.
#[cfg(test)]
pub(crate) enum IndexRangeIterator {
	Forward(IndexRangeForwardIterator),
	Backward(IndexRangeBackwardIterator),
}

#[cfg(test)]
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
	///
	/// Returns at most `limit` record ids, clamped to `1..=INDEX_BATCH_SIZE`.
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

// ---------------------------------------------------------------------------
// Unique-index range helpers
// ---------------------------------------------------------------------------

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
#[cfg(test)]
pub(crate) struct UniqueRangeForwardIterator {
	range: TypedRange<IndexEntryValue>,
}

#[cfg(test)]
impl UniqueRangeForwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let range = IndexKeyspace::of(ns, db, ix).range(from, to)?;

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
	///
	/// Returns at most `limit` record ids, clamped to `1..=INDEX_BATCH_SIZE`.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let r = scan(&mut self.range, tx, limit.clamp(1, INDEX_BATCH_SIZE)).await?;
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
#[cfg(test)]
pub(crate) struct UniqueRangeBackwardIterator {
	range: TypedRange<IndexEntryValue>,
}

#[cfg(test)]
impl UniqueRangeBackwardIterator {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let range = IndexKeyspace::of(ns, db, ix).range(from, to)?;

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
	///
	/// Returns at most `limit` record ids, clamped to `1..=INDEX_BATCH_SIZE`.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let r = scanr(&mut self.range, tx, limit.clamp(1, INDEX_BATCH_SIZE)).await?;
		decode_record_ids(r)
	}
}

/// Direction-dispatching wrapper for range scans on unique indexes.
///
/// Delegates to [`UniqueRangeForwardIterator`] or
/// [`UniqueRangeBackwardIterator`] depending on the [`Direction`]
/// provided at construction time.
#[cfg(test)]
pub(crate) enum UniqueRangeIterator {
	Forward(UniqueRangeForwardIterator),
	Backward(UniqueRangeBackwardIterator),
}

#[cfg(test)]
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
	///
	/// Returns at most `limit` record ids, clamped to `1..=INDEX_BATCH_SIZE`.
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

// ---------------------------------------------------------------------------
// Compound-index iterators
// ---------------------------------------------------------------------------

/// Iterator for compound (multi-column) index equality scans.
///
/// Supports both forward and backward scanning, controlled by [`Direction`].
/// Forward scans use `tx.scan()` and advance the `beg` cursor;
/// backward scans use `tx.scanr()` and retreat the `end` cursor.
#[cfg(test)]
pub(crate) struct CompoundEqualIterator {
	range: TypedRange<IndexEntryValue>,
	/// Scan direction
	direction: Direction,
}

#[cfg(test)]
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
		let range = compute_compound_key_range(ns, db, ix, prefix, range, None)?;
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
#[cfg(test)]
pub(crate) struct CompoundRangeForwardIterator {
	range: TypedRange<IndexEntryValue>,
}

#[cfg(test)]
impl CompoundRangeForwardIterator {
	/// Create a new compound range iterator.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		range: &(BinaryOperator, Value),
		upper: Option<&(BinaryOperator, Value)>,
	) -> Result<Self> {
		let range = compute_compound_key_range(ns, db, ix, prefix, Some(range), upper)?;
		Ok(Self {
			range,
		})
	}

	/// Fetch the next batch of record IDs in ascending key order, at most
	/// `limit` of them, clamped to `1..=INDEX_BATCH_SIZE` so a zero cap cannot
	/// read nothing and pass for exhaustion.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let scan_limit = limit.clamp(1, INDEX_BATCH_SIZE);
		let res = scan(&mut self.range, tx, scan_limit).await?;
		decode_record_ids(res)
	}
}

/// Direction-dispatching wrapper for compound range scans.
///
/// Delegates to [`CompoundRangeForwardIterator`] or
/// [`CompoundRangeBackwardIterator`] depending on the [`Direction`]
/// provided at construction time.
#[cfg(test)]
pub(crate) enum CompoundRangeIterator {
	Forward(CompoundRangeForwardIterator),
	Backward(CompoundRangeBackwardIterator),
}

#[cfg(test)]
impl CompoundRangeIterator {
	/// Create a new compound range iterator for the given direction.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		range: &(BinaryOperator, Value),
		upper: Option<&(BinaryOperator, Value)>,
		direction: Direction,
	) -> Result<Self> {
		match direction {
			Direction::Forward => Ok(Self::Forward(CompoundRangeForwardIterator::new(
				ns, db, ix, prefix, range, upper,
			)?)),
			Direction::Backward => Ok(Self::Backward(CompoundRangeBackwardIterator::new(
				ns, db, ix, prefix, range, upper,
			)?)),
		}
	}

	/// Fetch the next batch, delegating to the inner iterator.
	///
	/// Returns at most `limit` record ids, clamped to `1..=INDEX_BATCH_SIZE`.
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

/// A compound-index lookup: an equality prefix alone, or with a range on the
/// next column, so a scan drives either through one loop.
#[cfg(test)]
pub(crate) enum CompoundIterator {
	Equal(CompoundEqualIterator),
	Range(CompoundRangeIterator),
}

#[cfg(test)]
impl CompoundIterator {
	/// An iterator over the entries matching `prefix`, and `range` on the next
	/// column when it is given, in `direction`.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		range: Option<&(BinaryOperator, Value)>,
		upper: Option<&(BinaryOperator, Value)>,
		direction: Direction,
	) -> Result<Self> {
		Ok(match range {
			None => Self::Equal(CompoundEqualIterator::new(ns, db, ix, prefix, None, direction)?),
			Some(range) => Self::Range(CompoundRangeIterator::new(
				ns, db, ix, prefix, range, upper, direction,
			)?),
		})
	}

	/// Fetch the next batch: at most `limit` record ids, clamped to
	/// `1..=INDEX_BATCH_SIZE`.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		match self {
			Self::Equal(iter) => iter.next_batch(tx, limit).await,
			Self::Range(iter) => iter.next_batch(tx, limit).await,
		}
	}
}

/// The iterator a B-tree lookup takes for its access shape, so a caller drives
/// every shape through one loop.
#[cfg(test)]
pub(crate) enum BTreeIterator {
	UniqueEqual(UniqueEqualIterator),
	Equal(IndexEqualIterator),
	UniqueRange(UniqueRangeIterator),
	Range(IndexRangeIterator),
	Compound(CompoundIterator),
}

#[cfg(test)]
impl BTreeIterator {
	/// The iterator for `access` on `ix`, a unique index when `unique`, in
	/// `direction`; `None` for an access no B-tree iterator serves (full-text,
	/// KNN). A unique equality lookup ignores `direction`.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		unique: bool,
		access: &BTreeAccess,
		direction: Direction,
	) -> Result<Option<Self>> {
		use crate::exec::index::access_path::KeyAccess;
		Ok(Some(match (access.resolve(None)?, unique) {
			(Some(KeyAccess::Equality(value)), true) => {
				Self::UniqueEqual(UniqueEqualIterator::new(ns, db, ix, &value)?)
			}
			(Some(KeyAccess::Equality(value)), false) => {
				let reverse = matches!(direction, Direction::Backward);
				Self::Equal(IndexEqualIterator::with_direction(ns, db, ix, &value, reverse)?)
			}
			(Some(KeyAccess::Range(range)), true) => Self::UniqueRange(UniqueRangeIterator::new(
				ns,
				db,
				ix,
				range.start.as_ref(),
				range.end.as_ref(),
				direction,
			)?),
			(Some(KeyAccess::Range(range)), false) => Self::Range(IndexRangeIterator::new(
				ns,
				db,
				ix,
				range.start.as_ref(),
				range.end.as_ref(),
				direction,
			)?),
			(
				Some(KeyAccess::Compound {
					prefix,
					range,
					upper,
				}),
				_,
			) => Self::Compound(CompoundIterator::new(
				ns,
				db,
				ix,
				&prefix,
				range.as_ref(),
				upper.as_ref(),
				direction,
			)?),
			(None, _) => return Ok(None),
		}))
	}

	/// Fetch the next batch: at most `limit` record ids, clamped to
	/// `1..=INDEX_BATCH_SIZE`; an empty batch once the lookup is exhausted.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		match self {
			Self::UniqueEqual(iter) => iter.next_batch(tx, limit).await,
			Self::Equal(iter) => iter.next_batch(tx, limit).await,
			Self::UniqueRange(iter) => iter.next_batch(tx, limit).await,
			Self::Range(iter) => iter.next_batch(tx, limit).await,
			Self::Compound(iter) => iter.next_batch(tx, limit).await,
		}
	}
}

/// Backward iterator for compound (multi-column) index range scans.
///
/// Mirrors [`CompoundRangeForwardIterator`] but scans in descending key
/// order using `tx.scanr()`.  The `end` cursor retreats after each batch
/// while `beg` stays fixed, following the same pattern as
/// [`IndexRangeBackwardIterator`].
#[cfg(test)]
pub(crate) struct CompoundRangeBackwardIterator {
	range: TypedRange<IndexEntryValue>,
}

#[cfg(test)]
impl CompoundRangeBackwardIterator {
	/// Create a new backward compound range iterator.
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		range: &(BinaryOperator, Value),
		upper: Option<&(BinaryOperator, Value)>,
	) -> Result<Self> {
		let range = compute_compound_key_range(ns, db, ix, prefix, Some(range), upper)?;
		Ok(Self {
			range,
		})
	}

	/// Fetch the next batch of record IDs in descending key order, at most
	/// `limit` of them, clamped to `1..=INDEX_BATCH_SIZE` so a zero cap cannot
	/// read nothing and pass for exhaustion.
	pub(crate) async fn next_batch(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<Vec<RecordId>> {
		let scan_limit = limit.clamp(1, INDEX_BATCH_SIZE);

		let res = scanr(&mut self.range, tx, scan_limit).await?;

		decode_record_ids(res)
	}
}

/// Compute the KV range covering a B-tree access shape, for consumers that
/// drain raw entries directly (bitmap candidate scans).
///
/// Reuses the same range construction as the streaming iterators above, so a
/// drain sees exactly the entries the equivalent streaming scan would. A bound
/// that reads the statement clock resolves against `clock`, the instant of the
/// statement the drain runs for, or the wall clock where none is installed.
/// `FullText`/`Knn` access shapes are not B-tree scans and return an error.
pub(crate) fn bitmap_scan_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	access: &crate::exec::index::access_path::BTreeAccess,
	clock: Option<&crate::val::Datetime>,
) -> Result<TypedRange<IndexEntryValue>> {
	use crate::exec::index::access_path::KeyAccess;
	let unique = matches!(ix.index, crate::catalog::Index::Uniq);
	Ok(match access.resolve(clock)? {
		Some(KeyAccess::Equality(v)) => IndexKeyspace::of(ns, db, ix).equality(unique, &v)?,
		Some(KeyAccess::Range(range)) => {
			IndexKeyspace::of(ns, db, ix).range(range.start.as_ref(), range.end.as_ref())?
		}
		Some(KeyAccess::Compound {
			prefix,
			range,
			upper,
		}) => IndexKeyspace::of(ns, db, ix).compound_between(
			&prefix,
			range.as_ref().map(|(op, v)| (op, v)),
			upper.as_ref().map(|(op, v)| (op, v)),
		)?,
		None => {
			return Err(anyhow::anyhow!("Access shape is not a B-tree scan and has no key range"));
		}
	})
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// The entries of compound index `ix` under `prefix`, narrowed by `range` and
/// closed by `upper` on the same column ([`IndexKeyspace::compound_between`]).
#[cfg(test)]
fn compute_compound_key_range(
	ns: NamespaceId,
	db: DatabaseId,
	ix: &IndexDefinition,
	prefix: &[Value],
	range: Option<&(BinaryOperator, Value)>,
	upper: Option<&(BinaryOperator, Value)>,
) -> Result<TypedRange<IndexEntryValue>> {
	IndexKeyspace::of(ns, db, ix).compound_between(
		prefix,
		range.map(|(op, v)| (op, v)),
		upper.map(|(op, v)| (op, v)),
	)
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

	/// Drain an iterator whose `next_batch` takes a per-batch entry cap.
	///
	/// Returns the record ids in emission order plus the per-batch sizes, so a
	/// test can assert both the result and how it was chunked. An empty batch
	/// is the documented end-of-iteration signal.
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
		let (ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
		let (ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		assert_eq!(ids, reversed(&["t:a", "t:b"]));
	}

	#[tokio::test]
	async fn equality_on_an_absent_value_yields_nothing() {
		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let mut it = IndexEqualIterator::new(fx.ns, fx.db, fx.ix("iv"), &Value::from(99i64))
			.expect("equality iterator");
		let (ids, sizes) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
		let (none_ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		assert_eq!(none_ids, owned(&["t:e"]));

		let mut it = IndexEqualIterator::new(fx.ns, fx.db, fx.ix("iv"), &Value::Null)
			.expect("equality iterator");
		let (null_ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
		let (forward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
		let (backward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
			let (forward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
			assert_eq!(forward, owned(&expected), "forward {label}");

			let mut it =
				IndexRangeIterator::new(fx.ns, fx.db, fx.ix("iv"), from, to, Direction::Backward)
					.expect("range iterator");
			let (backward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
		let (ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
		let (forward, sizes) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		assert_eq!(sizes, vec![INDEX_BATCH_SIZE as usize, 100], "a batch caps at INDEX_BATCH_SIZE");
		assert_eq!(forward.len(), total);
		let unique: std::collections::HashSet<&String> = forward.iter().collect();
		assert_eq!(unique.len(), total, "no entry is returned twice across batches");
		assert_eq!(forward.first().map(String::as_str), Some("t:0"));
		assert_eq!(forward.last(), Some(&format!("t:{}", total - 1)));

		let mut it = IndexEqualIterator::with_direction(fx.ns, fx.db, fx.ix("iv"), &value, true)
			.expect("reverse equality iterator");
		let (backward, sizes) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
		let (ids, sizes) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		assert_eq!(ids, owned(&["u:2"]));
		assert_eq!(sizes, vec![1], "a unique key resolves in one batch");

		let mut it = UniqueEqualIterator::new(fx.ns, fx.db, fx.ix("ik"), &Value::from(21i64))
			.expect("unique equality iterator");
		let (ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
		let (ids, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
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
			let (forward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
			assert_eq!(forward, owned(&expected), "forward {label}");

			let mut it =
				UniqueRangeIterator::new(fx.ns, fx.db, fx.ix("ik"), from, to, Direction::Backward)
					.expect("unique range iterator");
			let (backward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
			assert_eq!(backward, reversed(&expected), "backward {label}");
		}
	}

	// ------------------------------------------------------------------
	// Compound (multi-column) index
	// ------------------------------------------------------------------

	/// `c` with a compound index on `(a, b)`; `c:4` sits under a different
	/// leading value so a prefix scan must not reach it.
	/// `BTreeIterator` builds the iterator each access shape needs and reads
	/// what that iterator reads; full-text and KNN access have none.
	#[tokio::test]
	async fn a_btree_iterator_follows_the_access_shape() {
		use crate::expr::operator::{BooleanOperator, MatchesOperator};
		use crate::val::Range;

		let from = |v: i64| Range {
			start: Bound::Included(Value::from(v)),
			end: Bound::Unbounded,
		};

		let db = uniq_db().await;
		let fx = Fixture::new(&db, "u").await;
		let tx = fx.tx();
		let mut it = BTreeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("ik"),
			true,
			&BTreeAccess::literal_equality(Value::from(20)),
			Direction::Forward,
		)
		.expect("iterator")
		.expect("a B-tree access");
		assert!(matches!(it, BTreeIterator::UniqueEqual(_)));
		assert_eq!(drain_capped!(it, tx, INDEX_BATCH_SIZE).0, owned(&["u:2"]));
		let mut it = BTreeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("ik"),
			true,
			&BTreeAccess::Range {
				range: from(20).into(),
			},
			Direction::Forward,
		)
		.expect("iterator")
		.expect("a B-tree access");
		assert!(matches!(it, BTreeIterator::UniqueRange(_)));
		assert_eq!(drain_capped!(it, tx, INDEX_BATCH_SIZE).0, owned(&["u:2", "u:3"]));

		let db = idx_db().await;
		let fx = Fixture::new(&db, "t").await;
		let tx = fx.tx();
		let mut it = BTreeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iv"),
			false,
			&BTreeAccess::literal_equality(Value::from(1)),
			Direction::Forward,
		)
		.expect("iterator")
		.expect("a B-tree access");
		assert!(matches!(it, BTreeIterator::Equal(_)));
		assert_eq!(drain_capped!(it, tx, INDEX_BATCH_SIZE).0, owned(&["t:a", "t:b"]));
		let mut it = BTreeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iv"),
			false,
			&BTreeAccess::Range {
				range: from(2).into(),
			},
			Direction::Forward,
		)
		.expect("iterator")
		.expect("a B-tree access");
		assert!(matches!(it, BTreeIterator::Range(_)));
		assert_eq!(drain_capped!(it, tx, INDEX_BATCH_SIZE).0, owned(&["t:c", "t:d"]));

		let db = compound_db().await;
		let fx = Fixture::new(&db, "c").await;
		let tx = fx.tx();
		let compound = |range, upper| BTreeAccess::Compound {
			prefix: vec![Value::from(1).into()],
			range,
			upper,
		};
		for (range, upper, ids) in [
			(None, None, vec!["c:1", "c:2", "c:3"]),
			(
				Some((BinaryOperator::MoreThanEqual, Value::from(2).into())),
				None,
				vec!["c:2", "c:3"],
			),
			(
				Some((BinaryOperator::MoreThanEqual, Value::from(1).into())),
				Some((BinaryOperator::LessThan, Value::from(3).into())),
				vec!["c:1", "c:2"],
			),
		] {
			let mut it = BTreeIterator::new(
				fx.ns,
				fx.db,
				fx.ix("iab"),
				false,
				&compound(range, upper),
				Direction::Forward,
			)
			.expect("iterator")
			.expect("a B-tree access");
			assert!(matches!(it, BTreeIterator::Compound(_)));
			assert_eq!(drain_capped!(it, tx, INDEX_BATCH_SIZE).0, owned(&ids));
		}

		let full_text = BTreeAccess::FullText {
			query: "x".to_owned(),
			operator: MatchesOperator {
				rf: None,
				operator: BooleanOperator::And,
			},
		};
		let knn = BTreeAccess::Knn {
			vector: Arc::from([]),
			k: 1,
			ef: 1,
		};
		for access in [full_text, knn] {
			let it =
				BTreeIterator::new(fx.ns, fx.db, fx.ix("iab"), false, &access, Direction::Forward)
					.expect("no error");
			assert!(it.is_none(), "no B-tree iterator serves {access:?}");
		}
	}

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
				None,
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
				None,
				Direction::Backward,
			)
			.expect("compound range iterator");
			let (backward, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
			assert_eq!(backward, reversed(&expected), "backward b {op:?} 2");
		}
	}

	#[tokio::test]
	async fn an_operator_that_is_no_comparison_is_refused() {
		// No key range answers an operator outside the comparisons, so the
		// iterator is refused rather than widened to every entry under the
		// prefix.
		let db = compound_db().await;
		let fx = Fixture::new(&db, "c").await;
		let prefix = vec![Value::from(1i64)];
		let range = (BinaryOperator::NotEqual, Value::from(2i64));
		let it = CompoundRangeIterator::new(
			fx.ns,
			fx.db,
			fx.ix("iab"),
			&prefix,
			&range,
			None,
			Direction::Forward,
		);
		assert!(it.is_err(), "`!=` must not answer the whole prefix");
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
	async fn zero_cap_reads_one_entry_on_every_compound_iterator() {
		// Every compound iterator clamps a zero cap up to 1: a 0-entry read
		// would be indistinguishable from exhaustion. The cap is not the
		// query's LIMIT; the scan pipeline tracks that itself and truncates
		// the batch.
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
		let mut it =
			CompoundRangeForwardIterator::new(fx.ns, fx.db, fx.ix("iab"), &prefix, &range, None)
				.expect("compound range iterator");
		let batch = it.next_batch(&tx, 0).await.expect("batch should scan");
		assert_eq!(batch.len(), 1);

		let mut it =
			CompoundRangeBackwardIterator::new(fx.ns, fx.db, fx.ix("iab"), &prefix, &range, None)
				.expect("compound range iterator");
		let batch = it.next_batch(&tx, 0).await.expect("batch should scan");
		assert_eq!(batch.len(), 1);
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
		let (streamed, _) = drain_capped!(it, tx, INDEX_BATCH_SIZE);
		assert_eq!(streamed.len(), 2, "the fixture has two rows on v = 1");

		let access = BTreeAccess::literal_equality(Value::from(1i64));
		let mut range = bitmap_scan_range(fx.ns, fx.db, ix, &access, None).expect("b-tree range");
		let entries = scan(&mut range, &tx, INDEX_BATCH_SIZE).await.expect("drain");
		let mut docs = roaring::RoaringTreemap::new();
		let mut missing = Vec::new();
		let decoded =
			decode_entry_doc_ids(entries, &mut docs, &mut missing, false, None).expect("decode");
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
		assert!(bitmap_scan_range(fx.ns, fx.db, ix, &full_text, None).is_err());

		let knn = BTreeAccess::Knn {
			vector: vec![Number::Int(1)].into(),
			k: 3,
			ef: 10,
		};
		assert!(bitmap_scan_range(fx.ns, fx.db, ix, &knn, None).is_err());
	}
}
