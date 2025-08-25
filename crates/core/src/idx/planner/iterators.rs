use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use anyhow::{Result, bail};
use radix_trie::Trie;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId};
use crate::ctx::Context;
use crate::err::Error;
use crate::expr::BinaryOperator;
use crate::idx::docids::DocId;
use crate::idx::ft::fulltext::FullTextHitsIterator;
use crate::idx::ft::search::SearchHitsIterator;
use crate::idx::planner::plan::StoreRangeValue;
use crate::idx::planner::tree::IndexReference;
use crate::key::index::Index;
use crate::key::value::{StoreKeyArray, StoreKeyValue};
use crate::kvs::{KVKey, Key, Transaction, Val};
use crate::val::record::Record;
use crate::val::{Array, RecordId, Value};

pub(crate) type IteratorRef = usize;

#[derive(Debug)]
pub(crate) struct IteratorRecord {
	irf: IteratorRef,
	doc_id: Option<DocId>,
	dist: Option<f64>,
}

impl IteratorRecord {
	pub(crate) fn irf(&self) -> IteratorRef {
		self.irf
	}
	pub(crate) fn doc_id(&self) -> Option<DocId> {
		self.doc_id
	}

	pub(crate) fn dist(&self) -> Option<f64> {
		self.dist
	}
}
impl From<IteratorRef> for IteratorRecord {
	fn from(irf: IteratorRef) -> Self {
		IteratorRecord {
			irf,
			doc_id: None,
			dist: None,
		}
	}
}

/// Abstraction over batch containers used by iterators (Vec or VecDeque),
/// allowing the same code to accumulate records regardless of concrete type.
pub(crate) trait IteratorBatch {
	fn empty() -> Self;
	fn with_capacity(capacity: usize) -> Self;
	fn from_one(record: IndexItemRecord) -> Self;
	fn add(&mut self, record: IndexItemRecord);
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool;
}

impl IteratorBatch for Vec<IndexItemRecord> {
	fn empty() -> Self {
		Vec::from([])
	}

	fn with_capacity(capacity: usize) -> Self {
		Vec::with_capacity(capacity)
	}
	fn from_one(record: IndexItemRecord) -> Self {
		Vec::from([record])
	}

	fn add(&mut self, record: IndexItemRecord) {
		self.push(record)
	}

	fn len(&self) -> usize {
		Vec::len(self)
	}

	fn is_empty(&self) -> bool {
		Vec::is_empty(self)
	}
}

impl IteratorBatch for VecDeque<IndexItemRecord> {
	fn empty() -> Self {
		VecDeque::from([])
	}
	fn with_capacity(capacity: usize) -> Self {
		VecDeque::with_capacity(capacity)
	}
	fn from_one(record: IndexItemRecord) -> Self {
		VecDeque::from([record])
	}

	fn add(&mut self, record: IndexItemRecord) {
		self.push_back(record)
	}

	fn len(&self) -> usize {
		VecDeque::len(self)
	}
	fn is_empty(&self) -> bool {
		VecDeque::is_empty(self)
	}
}

/// High-level iterator over index-backed scans which yields RecordIds (and
/// optionally pre-fetched Values) depending on the current RecordStrategy.
///
/// Each variant encapsulates a concrete scan strategy (equality, range, union,
/// join, text search, KNN, etc). Iteration is performed in batches to cap
/// per-IO work and allow cooperative cancellation via Context.
pub(crate) enum ThingIterator {
	IndexEqual(IndexEqualThingIterator),
	IndexRange(IndexRangeThingIterator),
	#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
	IndexRangeReverse(IndexRangeReverseThingIterator),
	IndexUnion(IndexUnionThingIterator),
	IndexJoin(Box<IndexJoinThingIterator>),
	UniqueEqual(UniqueEqualThingIterator),
	UniqueRange(UniqueRangeThingIterator),
	#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
	UniqueRangeReverse(UniqueRangeReverseThingIterator),
	UniqueUnion(UniqueUnionThingIterator),
	UniqueJoin(Box<UniqueJoinThingIterator>),
	SearchMatches(MatchesThingIterator<SearchHitsIterator>),
	FullTextMatches(MatchesThingIterator<FullTextHitsIterator>),
	Knn(KnnIterator),
}

impl ThingIterator {
	/// Fetch the next batch of index items.
	///
	/// - `size` is a soft upper bound on how many items to fetch. Concrete iterators may return
	///   fewer items (e.g., due to range boundaries) or, in rare edge-cases, one extra to honor
	///   inclusivity semantics when scanning in reverse.
	pub(crate) async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		size: u32,
	) -> Result<B> {
		match self {
			Self::IndexEqual(i) => i.next_batch(txn, size).await,
			Self::UniqueEqual(i) => i.next_batch(txn).await,
			Self::IndexRange(i) => i.next_batch(txn, size).await,
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			Self::IndexRangeReverse(i) => i.next_batch(txn, size).await,
			Self::UniqueRange(i) => i.next_batch(txn, size).await,
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			Self::UniqueRangeReverse(i) => i.next_batch(txn, size).await,
			Self::IndexUnion(i) => i.next_batch(ctx, txn, size).await,
			Self::UniqueUnion(i) => i.next_batch(ctx, txn, size).await,
			Self::SearchMatches(i) => i.next_batch(ctx, txn, size).await,
			Self::FullTextMatches(i) => i.next_batch(ctx, txn, size).await,
			Self::Knn(i) => i.next_batch(ctx, size).await,
			Self::IndexJoin(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
			Self::UniqueJoin(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
		}
	}

	/// Count up to the next `size` matching items without materializing values.
	///
	/// Used for SELECT ... COUNT and for explain paths where only cardinality
	/// is required.
	pub(crate) async fn next_count(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		size: u32,
	) -> Result<usize> {
		match self {
			Self::IndexEqual(i) => i.next_count(txn, size).await,
			Self::UniqueEqual(i) => i.next_count(txn).await,
			Self::IndexRange(i) => i.next_count(txn, size).await,
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			Self::IndexRangeReverse(i) => i.next_count(txn, size).await,
			Self::UniqueRange(i) => i.next_count(txn, size).await,
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			Self::UniqueRangeReverse(i) => i.next_count(txn, size).await,
			Self::IndexUnion(i) => i.next_count(ctx, txn, size).await,
			Self::UniqueUnion(i) => i.next_count(ctx, txn, size).await,
			Self::SearchMatches(i) => i.next_count(ctx, txn, size).await,
			Self::FullTextMatches(i) => i.next_count(ctx, txn, size).await,
			Self::Knn(i) => i.next_count(ctx, size).await,
			Self::IndexJoin(i) => Box::pin(i.next_count(ctx, txn, size)).await,
			Self::UniqueJoin(i) => Box::pin(i.next_count(ctx, txn, size)).await,
		}
	}
}

/// Iterator output record. Either a key-only result (for index-only scans)
/// or a key+value pair when values are fetched by the current RecordStrategy.
pub(crate) enum IndexItemRecord {
	/// We just collected the key
	Key(Arc<RecordId>, IteratorRecord),
	/// We have collected the key and the value
	KeyValue(Arc<RecordId>, Arc<Record>, IteratorRecord),
}

impl IndexItemRecord {
	fn new(t: Arc<RecordId>, ir: IteratorRecord, val: Option<Arc<Record>>) -> Self {
		if let Some(val) = val {
			Self::KeyValue(t, val, ir)
		} else {
			Self::Key(t, ir)
		}
	}

	fn new_key(t: RecordId, ir: IteratorRecord) -> Self {
		Self::Key(Arc::new(t), ir)
	}
	fn thing(&self) -> &RecordId {
		match self {
			Self::Key(t, _) => t,
			Self::KeyValue(t, _, _) => t,
		}
	}

	pub(crate) fn consume(self) -> (Arc<RecordId>, Option<Arc<Record>>, IteratorRecord) {
		match self {
			Self::Key(t, ir) => (t, None, ir),
			Self::KeyValue(t, v, ir) => (t, Some(v), ir),
		}
	}
}

pub(crate) struct IndexEqualThingIterator {
	irf: IteratorRef,
	beg: Vec<u8>,
	end: Vec<u8>,
}

impl IndexEqualThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fd: &StoreKeyArray,
	) -> Result<Self> {
		let (beg, end) = Self::get_beg_end(ns, db, ix, fd)?;
		Ok(Self {
			irf,
			beg,
			end,
		})
	}

	/// Computes the begin and end keys for scanning an equality index.
	///
	/// For single-column indexes, uses simple prefix key generation.
	/// For composite indexes (multiple columns), uses composite key generation
	/// which handles the ordering and encoding of multiple index values.
	///
	/// Returns a tuple of (begin_key, end_key) that defines the scan range
	/// for finding all records that exactly match the provided array values.
	fn get_beg_end(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fd: &StoreKeyArray,
	) -> Result<(Vec<u8>, Vec<u8>)> {
		Ok(if ix.cols.len() == 1 {
			// Single column index: straightforward key prefix generation
			(
				Index::prefix_ids_beg(ns, db, &ix.what, &ix.name, fd)?,
				Index::prefix_ids_end(ns, db, &ix.what, &ix.name, fd)?,
			)
		} else {
			// Composite index: handles multiple column values with proper ordering
			(
				Index::prefix_ids_composite_beg(ns, db, &ix.what, &ix.name, fd)?,
				Index::prefix_ids_composite_end(ns, db, &ix.what, &ix.name, fd)?,
			)
		})
	}

	/// Performs a key-value scan within the specified range and updates the
	/// begin key for pagination.
	///
	/// This method scans the key-value store between `beg` and `end` keys,
	/// returning up to `limit` results. After scanning, it updates the `beg`
	/// key to continue from where this scan left off, enabling
	/// efficient pagination through large result sets.
	///
	/// The key manipulation (appending 0x00) ensures that the next scan will
	/// start after the last key returned, avoiding duplicate results while
	/// maintaining correct lexicographic ordering.
	async fn next_scan(
		tx: &Transaction,
		beg: &mut Vec<u8>,
		end: &[u8],
		limit: u32,
	) -> Result<Vec<(Key, Val)>> {
		let min = beg.clone();
		let max = end.to_owned();
		let res = tx.scan(min..max, limit, None).await?;
		// Update the begin key for the next scan to avoid duplicates and enable
		// pagination
		if let Some((key, _)) = res.last() {
			let mut key = key.clone();
			key.push(0x00); // Move to the next possible key lexicographically
			*beg = key;
		}
		Ok(res)
	}

	async fn next_scan_batch<B: IteratorBatch>(
		tx: &Transaction,
		irf: IteratorRef,
		beg: &mut Vec<u8>,
		end: &[u8],
		limit: u32,
	) -> Result<B> {
		let res = Self::next_scan(tx, beg, end, limit).await?;
		let mut records = B::with_capacity(res.len());
		res.into_iter().try_for_each(|(_, val)| -> Result<()> {
			records.add(IndexItemRecord::new_key(revision::from_slice(&val)?, irf.into()));
			Ok(())
		})?;
		Ok(records)
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction, limit: u32) -> Result<B> {
		Self::next_scan_batch(tx, self.irf, &mut self.beg, &self.end, limit).await
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize> {
		Ok(Self::next_scan(tx, &mut self.beg, &self.end, limit).await?.len())
	}
}

struct RangeScan {
	beg: Key,
	end: Key,
	/// True if the beginning key has already been seen and match checked
	beg_excl_match_checked: bool,
	/// True if the ending key has already been seen and match checked
	end_excl_match_checked: bool,
}

impl RangeScan {
	fn new(beg_key: Key, beg_incl: bool, end_key: Key, end_incl: bool) -> Self {
		Self {
			beg: beg_key,
			end: end_key,
			beg_excl_match_checked: beg_incl,
			end_excl_match_checked: end_incl,
		}
	}

	fn range(&self) -> Range<Key> {
		self.beg.clone()..self.end.clone()
	}

	/// Determines whether a given key should be included in the range scan
	/// results.
	///
	/// This method implements inclusive/exclusive boundary logic for range
	/// scans. It tracks whether boundary keys have been encountered and
	/// applies the appropriate inclusion/exclusion rules based on the range
	/// configuration.
	///
	/// Returns `false` for keys that should be excluded (boundary keys when the
	/// range is exclusive at that boundary), `true` for keys that should be
	/// included.
	fn matches(&mut self, k: &Key) -> bool {
		// Handle beginning boundary: exclude if this is an exclusive range start
		if !self.beg_excl_match_checked && self.beg.eq(k) {
			self.beg_excl_match_checked = true;
			return false; // Exclude the boundary key for exclusive ranges
		}
		// Handle ending boundary: exclude if this is an exclusive range end
		if !self.end_excl_match_checked && self.end.eq(k) {
			self.end_excl_match_checked = true;
			return false; // Exclude the boundary key for exclusive ranges
		}
		true // Include all other keys within the range
	}

	fn matches_end(&mut self) -> bool {
		// We check if we should match the key matching the end of the range
		if !self.end_excl_match_checked && self.end.eq(&self.end) {
			self.end_excl_match_checked = true;
			return false;
		}
		true
	}
}

#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
struct ReverseRangeScan {
	r: RangeScan,
	/// True if the beginning key should be included
	beg_incl: bool,
	/// True if the ending key should be included
	end_incl: bool,
}

#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
impl ReverseRangeScan {
	fn new(r: RangeScan) -> Self {
		// Capture whether the original forward range considered the endpoints inclusive.
		// Reverse KV scans typically exclude the end key, so we keep these flags and
		// later compensate by explicitly fetching the endpoint once per iterator.
		Self {
			beg_incl: r.beg_excl_match_checked,
			end_incl: r.end_excl_match_checked,
			r,
		}
	}
	fn matches_check(&self, k: &Key) -> bool {
		// Skip keys that are exactly equal to the range boundaries if we haven't
		// performed the explicit endpoint compensation yet. This avoids double
		// returning the endpoints when they are inclusive.
		if !self.r.beg_excl_match_checked && self.r.beg.eq(k) {
			return false;
		}
		if !self.r.end_excl_match_checked && self.r.end.eq(k) {
			return false;
		}
		true
	}
}

pub(crate) struct IndexRangeThingIterator {
	irf: IteratorRef,
	r: RangeScan,
}

impl IndexRangeThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: StoreRangeValue,
		to: StoreRangeValue,
	) -> Result<Self> {
		Ok(Self {
			irf,
			r: Self::range_scan(ns, db, ix, from, to)?,
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<Self> {
		Self::new(irf, ns, db, ix, StoreRangeValue::default(), StoreRangeValue::default())
	}

	pub(super) fn compound_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexReference,
		prefix: &[Value],
		ranges: &[(BinaryOperator, Arc<Value>)],
	) -> Result<Self> {
		let (from, to) = Self::reduce_range(ranges)?;
		Ok(Self {
			irf,
			r: Self::range_scan_prefix(ns, db, ix, prefix, from, to)?,
		})
	}

	/// Determines the lowest and highest values in the range
	fn reduce_range(
		ranges: &[(BinaryOperator, Arc<Value>)],
	) -> Result<(StoreRangeValue, StoreRangeValue)> {
		let mut from = vec![];
		let mut to = vec![];
		for (op, v) in ranges {
			let v: StoreKeyValue = v.as_ref().clone().into();
			let key = v.encode_key()?;
			match op {
				BinaryOperator::LessThan => to.push((key, false, v)),
				BinaryOperator::LessThanEqual => to.push((key, true, v.clone())),
				BinaryOperator::MoreThan => from.push((key, true, v.clone())),
				BinaryOperator::MoreThanEqual => from.push((key, false, v.clone())),
				_ => {
					bail!(Error::Unreachable(format!("Invalid operator for range extraction {op}")))
				}
			}
		}
		// Sort candidates by encoded key. For lower bounds we want the greatest value (max),
		// for upper bounds we want the smallest (min). The comparator orders by key descending
		// (b1.cmp(a1)), and for equal keys orders by the boolean flag so that strict operators
		// take precedence when choosing the tightest bound.
		let cmp = |(a1, a2, _): &(Vec<u8>, bool, StoreKeyValue),
		           (b1, b2, _): &(Vec<u8>, bool, StoreKeyValue)| {
			b1.cmp(a1).then_with(|| b2.cmp(a2))
		};
		from.sort_unstable_by(cmp);
		to.sort_unstable_by(cmp);
		// Pick the strongest lower bound: first element after sorting (greatest key).
		// The stored boolean reflects the original operator kind: true for strict (>, <),
		// false for inclusive (>=, <=). For the final bound, inclusive is the inverse for
		// lower bounds because a strict '>' becomes an exclusive range start.
		let from = if let Some((_, inclusivity, val)) = from.into_iter().next() {
			StoreRangeValue {
				value: val,
				inclusive: !inclusivity,
			}
		} else {
			StoreRangeValue::default()
		};
		// Pick the strongest upper bound: last element after sorting (smallest key).
		// Here the inclusive flag matches the operator: '<=' is inclusive, '<' is exclusive.
		let to = if let Some((_, inclusivity, val)) = to.into_iter().next_back() {
			StoreRangeValue {
				value: val,
				inclusive: inclusivity,
			}
		} else {
			StoreRangeValue::default()
		};
		Ok((from, to))
	}

	fn range_scan(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: StoreRangeValue,
		to: StoreRangeValue,
	) -> Result<RangeScan> {
		let (from_inclusive, to_inclusive) = (from.inclusive, to.inclusive);
		let beg = Self::compute_beg(ns, db, &ix.what, &ix.name, from)?;
		let end = Self::compute_end(ns, db, &ix.what, &ix.name, to)?;
		Ok(RangeScan::new(beg, from_inclusive, end, to_inclusive))
	}

	/// Compute the begin key for a range scan over an index by value.
	///
	/// - If `from.value` is `None`, use the index-prefix begin to start at the first key in the
	///   index keyspace.
	/// - Otherwise, serialize the `from` value into an index field array and construct the boundary
	///   key. For an inclusive lower bound use `prefix_ids_beg` (include all records with that
	///   value); for an exclusive lower bound use `prefix_ids_end` so the scan starts after all
	///   records with that exact value.
	fn compute_beg(
		ns: NamespaceId,
		db: DatabaseId,
		ix_what: &str,
		ix_name: &str,
		from: StoreRangeValue,
	) -> Result<Vec<u8>> {
		if from.value.is_none() {
			return Index::prefix_beg(ns, db, ix_what, ix_name);
		}
		if from.inclusive {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &StoreKeyArray::from(from.value))
		} else {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &StoreKeyArray::from(from.value))
		}
	}

	/// Compute the end key for a range scan over an index by value.
	///
	/// - If `to.value` is `None`, use the index-prefix end to stop at the last key in the index
	///   keyspace.
	/// - Otherwise, serialize the `to` value and construct the boundary key. For an inclusive upper
	///   bound use `prefix_ids_end` so the scan can include all records with that exact value; for
	///   an exclusive upper bound use `prefix_ids_beg` so the scan stops just before any key
	///   matching that exact value.
	fn compute_end(
		ns: NamespaceId,
		db: DatabaseId,
		ix_what: &str,
		ix_name: &str,
		to: StoreRangeValue,
	) -> Result<Vec<u8>> {
		if to.value.is_none() {
			return Index::prefix_end(ns, db, ix_what, ix_name);
		}
		if to.inclusive {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &StoreKeyArray::from(to.value))
		} else {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &StoreKeyArray::from(to.value))
		}
	}

	/// Build a range scan over a composite index using a fixed `prefix` and
	/// an optional range on the next column value.
	///
	/// - When `from` or `to` values are `None`, we scan the full extent of the composite tuple
	///   starting at `prefix` by using the composite begin/end sentinels.
	/// - When values are provided, we append them to the prefix and construct inclusive/exclusive
	///   boundaries using the appropriate prefix functions.
	fn range_scan_prefix(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		prefix: &[Value],
		from: StoreRangeValue,
		to: StoreRangeValue,
	) -> Result<RangeScan> {
		// Prepare the fixed composite prefix (may be empty for the leading column)
		let prefix_array: StoreKeyArray = if prefix.is_empty() {
			Array(Vec::with_capacity(1))
		} else {
			Array::from(prefix.to_vec())
		}
		.into();
		let (from_inclusive, to_inclusive) = (from.inclusive, to.inclusive);
		// Compute the lower bound for the scan
		let beg = if from.value.is_none() {
			Index::prefix_ids_composite_beg(ns, db, &ix.what, &ix.name, &prefix_array)?
		} else {
			Self::compute_beg_with_prefix(ns, db, &ix.what, &ix.name, &prefix_array, from)?
		};
		// Compute the upper bound for the scan
		let end = if to.value.is_none() {
			Index::prefix_ids_composite_end(ns, db, &ix.what, &ix.name, &prefix_array)?
		} else {
			Self::compute_end_with_prefix(ns, db, &ix.what, &ix.name, &prefix_array, to)?
		};
		Ok(RangeScan::new(beg, from_inclusive, end, to_inclusive))
	}

	/// Compute the begin key for a composite index range when a fixed `prefix`
	/// (values for leading columns) is provided and an optional `from`
	/// value applies to the next column.
	///
	/// Inclusive `from` uses `prefix_ids_beg` to include all rows equal to the
	/// boundary value; exclusive `from` uses `prefix_ids_end` to start just
	/// after all keys equal to that boundary.
	fn compute_beg_with_prefix(
		ns: NamespaceId,
		db: DatabaseId,
		ix_what: &str,
		ix_name: &str,
		prefix: &StoreKeyArray,
		from: StoreRangeValue,
	) -> Result<Vec<u8>> {
		let mut fd = prefix.clone();
		fd.0.push(from.value);
		if from.inclusive {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &fd)
		} else {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &fd)
		}
	}

	/// Compute the end key for a composite index range when a fixed `prefix`
	/// is provided and an optional `to` value applies to the next
	/// column.
	///
	/// Inclusive `to` uses `prefix_ids_end` so rows equal to the boundary are
	/// still reachable by the scan; exclusive `to` uses `prefix_ids_beg` to
	/// stop just before any key matching that boundary value.
	fn compute_end_with_prefix(
		ns: NamespaceId,
		db: DatabaseId,
		ix_what: &str,
		ix_name: &str,
		prefix: &StoreKeyArray,
		to: StoreRangeValue,
	) -> Result<Vec<u8>> {
		let mut fd = prefix.clone();
		fd.0.push(to.value);
		if to.inclusive {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &fd)
		} else {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &fd)
		}
	}

	/// Scan key-value pairs within the current range, up to `limit`, and
	/// advance the begin key to resume pagination without duplicates.
	///
	/// We update `self.r.beg` to be one byte past the last returned key
	/// (by appending 0x00), which works with lexicographic ordering to ensure
	/// the next call starts strictly after the last result.
	async fn next_scan(&mut self, tx: &Transaction, limit: u32) -> Result<Vec<(Key, Val)>> {
		let res = tx.scan(self.r.range(), limit, None).await?;
		if let Some((key, _)) = res.last() {
			self.r.beg.clone_from(key);
			// Advance begin key one byte past the last returned key to avoid
			// returning it again on the next paged call. Since keys are
			// lexicographically ordered, appending 0x00 moves strictly after `key`.
			self.r.beg.push(0x00);
		}
		Ok(res)
	}

	/// Scan only the keys within the current range, up to `limit`, and advance
	/// the begin key to resume on the next call without duplicates. This
	/// mirrors `next_scan` but avoids fetching values for count-only
	/// operations.
	async fn next_keys(&mut self, tx: &Transaction, limit: u32) -> Result<Vec<Key>> {
		let res = tx.keys(self.r.range(), limit, None).await?;
		if let Some(key) = res.last() {
			self.r.beg.clone_from(key);
			// Same pagination technique as in next_scan: move begin strictly past
			// the last seen key so subsequent calls don't re-count it.
			self.r.beg.push(0x00);
		}
		Ok(res)
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction, limit: u32) -> Result<B> {
		let res = self.next_scan(tx, limit).await?;
		let mut records = B::with_capacity(res.len());
		res.into_iter().filter(|(k, _)| self.r.matches(k)).try_for_each(
			|(_, v)| -> Result<()> {
				records.add(IndexItemRecord::new_key(revision::from_slice(&v)?, self.irf.into()));
				Ok(())
			},
		)?;
		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize> {
		let res = self.next_keys(tx, limit).await?;
		let count = res.into_iter().filter(|k| self.r.matches(k)).count();
		Ok(count)
	}
}

#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
pub(crate) struct IndexRangeReverseThingIterator {
	irf: IteratorRef,
	r: ReverseRangeScan,
}

#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
impl IndexRangeReverseThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: StoreRangeValue,
		to: StoreRangeValue,
	) -> Result<Self> {
		Ok(Self {
			irf,
			r: ReverseRangeScan::new(IndexRangeThingIterator::range_scan(ns, db, ix, from, to)?),
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<Self> {
		Self::new(irf, ns, db, ix, StoreRangeValue::default(), StoreRangeValue::default())
	}
	/// When scanning in reverse, the KV range APIs do not return the inclusive
	/// end key. We compensate by explicitly checking and returning the end key
	/// once per iterator state, decrementing the remaining `limit` accordingly.
	async fn check_batch_ending(
		&mut self,
		tx: &Transaction,
		limit: &mut u32,
	) -> Result<Option<IndexItemRecord>> {
		if !self.r.end_incl || !self.r.matches_check(&self.r.r.end) {
			return Ok(None);
		}
		self.r.r.end_excl_match_checked = true;
		if let Some(v) = tx.get(&self.r.r.end, None).await? {
			*limit -= 1;
			Ok(Some(IndexItemRecord::new_key(revision::from_slice(&v)?, self.irf.into())))
		} else {
			Ok(None)
		}
	}

	async fn check_keys_ending(&mut self, tx: &Transaction, limit: &mut u32) -> Result<bool> {
		if !self.r.end_incl || !self.r.matches_check(&self.r.r.end) {
			return Ok(false);
		}
		self.r.r.end_excl_match_checked = true;
		if tx.exists(&self.r.r.end, None).await? {
			*limit -= 1;
			Ok(true)
		} else {
			Ok(false)
		}
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<B> {
		// Check if we need to retrieve the key at end of the range (not returned by the
		// scanr)
		let ending = self.check_batch_ending(tx, &mut limit).await?;

		// Do we have enough limit left to collect additional records?
		let res = if limit > 0 {
			tx.scanr(self.r.r.range(), limit, None).await?
		} else {
			vec![]
		};

		// Proper allocation for the result
		let mut records = B::with_capacity(res.len() + (ending.is_some() as usize));

		// Add the ending record if any
		if let Some(r) = ending {
			records.add(r);
		}

		// Collect the last key
		let last_key = res.last().map(|(k, _)| k.clone());

		// Feed the result
		res.into_iter().filter(|(k, _)| self.r.r.matches(k)).try_for_each(
			|(_, v)| -> Result<()> {
				records.add(IndexItemRecord::new_key(revision::from_slice(&v)?, self.irf.into()));
				Ok(())
			},
		)?;

		// Update the ending for the next batch
		if let Some(key) = last_key {
			self.r.r.end = key;
		}

		// The next batch should not include the end anymore
		if self.r.end_incl {
			self.r.end_incl = false;
		}
		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, mut limit: u32) -> Result<usize> {
		// Check if we need to retrieve the key at end of the range (not returned by the
		// keysr)
		let mut count = self.check_keys_ending(tx, &mut limit).await? as usize;

		// Do we have enough limit left to collect additional records?
		let res = if limit > 0 {
			tx.keysr(self.r.r.range(), limit, None).await?
		} else {
			vec![]
		};

		// Feed the result
		count += res.iter().filter(|k| self.r.r.matches(k)).count();

		// Update the ending for the next batch
		if let Some(key) = res.last() {
			self.r.r.end.clone_from(key);
		}

		// The next batch should not include the end anymore
		if self.r.end_incl {
			self.r.end_incl = false;
		}
		Ok(count)
	}
}

pub(crate) struct IndexUnionThingIterator {
	irf: IteratorRef,
	values: VecDeque<(Vec<u8>, Vec<u8>)>,
	current: Option<(Vec<u8>, Vec<u8>)>,
}

impl IndexUnionThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fds: &[StoreKeyArray],
	) -> Result<Self> {
		// We create a VecDeque to hold the prefix keys (begin and end) for each value
		// in the array.
		let mut values: VecDeque<(Vec<u8>, Vec<u8>)> = VecDeque::with_capacity(fds.len());

		for fd in fds {
			let (beg, end) = IndexEqualThingIterator::get_beg_end(ns, db, ix, fd)?;
			values.push_back((beg, end));
		}
		let current = values.pop_front();
		Ok(Self {
			irf,
			values,
			current,
		})
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B> {
		while let Some(r) = &mut self.current {
			if ctx.is_done(true).await? {
				break;
			}
			let records: B =
				IndexEqualThingIterator::next_scan_batch(tx, self.irf, &mut r.0, &r.1, limit)
					.await?;
			if !records.is_empty() {
				return Ok(records);
			}
			self.current = self.values.pop_front();
		}
		Ok(B::empty())
	}

	async fn next_count(&mut self, ctx: &Context, tx: &Transaction, limit: u32) -> Result<usize> {
		while let Some(r) = &mut self.current {
			if ctx.is_done(true).await? {
				break;
			}
			let res = IndexEqualThingIterator::next_scan(tx, &mut r.0, &r.1, limit).await?;
			if !res.is_empty() {
				return Ok(res.len());
			}
			self.current = self.values.pop_front();
		}
		Ok(0)
	}
}

struct JoinThingIterator {
	ns: NamespaceId,
	db: DatabaseId,
	ix: IndexReference,
	remote_iterators: VecDeque<ThingIterator>,
	current_remote: Option<ThingIterator>,
	current_remote_batch: VecDeque<IndexItemRecord>,
	current_local: Option<ThingIterator>,
	distinct: Trie<Key, bool>,
}

impl JoinThingIterator {
	pub(super) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: IndexReference,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Result<Self> {
		Ok(Self {
			ns,
			db,
			ix,
			current_remote: None,
			current_remote_batch: VecDeque::with_capacity(1),
			remote_iterators,
			current_local: None,
			distinct: Default::default(),
		})
	}
}

impl JoinThingIterator {
	async fn next_current_remote_batch(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<bool> {
		while !ctx.is_done(true).await? {
			if let Some(it) = &mut self.current_remote {
				self.current_remote_batch = it.next_batch(ctx, tx, limit).await?;
				if !self.current_remote_batch.is_empty() {
					return Ok(true);
				}
			}
			self.current_remote = self.remote_iterators.pop_front();
			if self.current_remote.is_none() {
				break;
			}
		}
		Ok(false)
	}

	async fn next_current_local<F>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
		new_iter: F,
	) -> Result<bool>
	where
		F: Fn(NamespaceId, DatabaseId, &IndexDefinition, StoreKeyValue) -> Result<ThingIterator>,
	{
		while !ctx.is_done(true).await? {
			let mut count = 0;
			while let Some(r) = self.current_remote_batch.pop_front() {
				if ctx.is_done(count % 100 == 0).await? {
					break;
				}
				let thing = r.thing();
				let value: StoreKeyValue = Value::from(thing.clone()).into();
				let k: Key = revision::to_vec(thing)?;
				if self.distinct.insert(k, true).is_none() {
					self.current_local = Some(new_iter(self.ns, self.db, &self.ix, value)?);
					return Ok(true);
				}
				count += 1;
			}
			if !self.next_current_remote_batch(ctx, tx, limit).await? {
				break;
			}
		}
		Ok(false)
	}

	async fn next_batch<F, B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
		new_iter: F,
	) -> Result<B>
	where
		F: Fn(NamespaceId, DatabaseId, &IndexDefinition, StoreKeyValue) -> Result<ThingIterator>
			+ Copy,
	{
		while !ctx.is_done(true).await? {
			if let Some(current_local) = &mut self.current_local {
				let records: B = current_local.next_batch(ctx, tx, limit).await?;
				if !records.is_empty() {
					return Ok(records);
				}
			}
			if !self.next_current_local(ctx, tx, limit, new_iter).await? {
				break;
			}
		}
		Ok(B::empty())
	}

	async fn next_count<F>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
		new_iter: F,
	) -> Result<usize>
	where
		F: Fn(NamespaceId, DatabaseId, &IndexDefinition, StoreKeyValue) -> Result<ThingIterator>
			+ Copy,
	{
		while !ctx.is_done(true).await? {
			if let Some(current_local) = &mut self.current_local {
				let count = current_local.next_count(ctx, tx, limit).await?;
				if count > 0 {
					return Ok(count);
				}
			}
			if !self.next_current_local(ctx, tx, limit, new_iter).await? {
				break;
			}
		}
		Ok(0)
	}
}

pub(crate) struct IndexJoinThingIterator(IteratorRef, JoinThingIterator);

impl IndexJoinThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: IndexReference,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Result<Self> {
		Ok(Self(irf, JoinThingIterator::new(ns, db, ix, remote_iterators)?))
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B> {
		let new_iter =
			|ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition, value: StoreKeyValue| {
				let fd = StoreKeyArray::from(value);
				let it = IndexEqualThingIterator::new(self.0, ns, db, ix, &fd)?;
				Ok(ThingIterator::IndexEqual(it))
			};
		self.1.next_batch(ctx, tx, limit, new_iter).await
	}

	async fn next_count(&mut self, ctx: &Context, tx: &Transaction, limit: u32) -> Result<usize> {
		let new_iter =
			|ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition, value: StoreKeyValue| {
				let fd = StoreKeyArray::from(value);
				let it = IndexEqualThingIterator::new(self.0, ns, db, ix, &fd)?;
				Ok(ThingIterator::IndexEqual(it))
			};
		self.1.next_count(ctx, tx, limit, new_iter).await
	}
}

pub(crate) struct UniqueEqualThingIterator {
	irf: IteratorRef,
	key: Option<Key>,
}

impl UniqueEqualThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		a: &StoreKeyArray,
	) -> Result<Self> {
		let key = Index::new(ns, db, &ix.what, &ix.name, a, None).encode_key()?;
		Ok(Self {
			irf,
			key: Some(key),
		})
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction) -> Result<B> {
		if let Some(key) = self.key.take() {
			if let Some(val) = tx.get(&key, None).await? {
				let rid: RecordId = revision::from_slice(&val)?;
				let record = IndexItemRecord::new_key(rid, self.irf.into());
				return Ok(B::from_one(record));
			}
		}
		Ok(B::empty())
	}

	async fn next_count(&mut self, tx: &Transaction) -> Result<usize> {
		if let Some(key) = self.key.take() {
			if tx.exists(&key, None).await? {
				return Ok(1);
			}
		}
		Ok(0)
	}
}

pub(crate) struct UniqueRangeThingIterator {
	irf: IteratorRef,
	r: RangeScan,
	done: bool,
}

impl UniqueRangeThingIterator {
	fn range_scan(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: StoreRangeValue,
		to: StoreRangeValue,
	) -> Result<RangeScan> {
		let beg = Self::compute_beg(ns, db, &ix.what, &ix.name, from.value)?;
		let end = Self::compute_end(ns, db, &ix.what, &ix.name, to.value)?;
		Ok(RangeScan::new(beg, from.inclusive, end, to.inclusive))
	}

	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: StoreRangeValue,
		to: StoreRangeValue,
	) -> Result<Self> {
		let r = Self::range_scan(ns, db, ix, from, to)?;
		Ok(Self {
			irf,
			r,
			done: false,
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<Self> {
		Self::new(irf, ns, db, ix, StoreRangeValue::default(), StoreRangeValue::default())
	}

	pub(super) fn compound_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexReference,
		prefix: &[Value],
		ranges: &[(BinaryOperator, Arc<Value>)],
	) -> Result<Self> {
		let (from, to) = IndexRangeThingIterator::reduce_range(ranges)?;
		let r = IndexRangeThingIterator::range_scan_prefix(ns, db, ix, prefix, from, to)?;
		Ok(Self {
			irf,
			r,
			done: false,
		})
	}

	fn compute_beg(
		ns: NamespaceId,
		db: DatabaseId,
		ix_what: &str,
		ix_name: &str,
		from: StoreKeyValue,
	) -> Result<Vec<u8>> {
		if from.is_none() {
			return Index::prefix_beg(ns, db, ix_what, ix_name);
		}
		Index::new(ns, db, ix_what, ix_name, &StoreKeyArray::from(from), None).encode_key()
	}

	fn compute_end(
		ns: NamespaceId,
		db: DatabaseId,
		ix_what: &str,
		ix_name: &str,
		to: StoreKeyValue,
	) -> Result<Vec<u8>> {
		if to.is_none() {
			return Index::prefix_end(ns, db, ix_what, ix_name);
		}
		Index::new(ns, db, ix_what, ix_name, &StoreKeyArray::from(to), None).encode_key()
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<B> {
		if self.done {
			return Ok(B::empty());
		}
		limit += 1;
		let res = tx.scan(self.r.range(), limit, None).await?;
		let mut records = B::with_capacity(res.len());
		for (k, v) in res {
			limit -= 1;
			if limit == 0 {
				self.r.beg = k;
				return Ok(records);
			}
			if self.r.matches(&k) {
				let rid: RecordId = revision::from_slice(&v)?;
				records.add(IndexItemRecord::new_key(rid, self.irf.into()));
			}
		}

		if self.r.matches_end() {
			if let Some(v) = tx.get(&self.r.end, None).await? {
				let rid: RecordId = revision::from_slice(&v)?;
				records.add(IndexItemRecord::new_key(rid, self.irf.into()));
			}
		}
		self.done = true;
		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, mut limit: u32) -> Result<usize> {
		if self.done {
			return Ok(0);
		}
		limit += 1;
		let res = tx.keys(self.r.range(), limit, None).await?;
		let mut count = 0;
		for k in res {
			limit -= 1;
			if limit == 0 {
				self.r.beg = k;
				return Ok(count);
			}
			if self.r.matches(&k) {
				count += 1;
			}
		}
		if self.r.matches_end() && tx.exists(&self.r.end, None).await? {
			count += 1;
		}
		self.done = true;
		Ok(count)
	}
}

#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
pub(crate) struct UniqueRangeReverseThingIterator {
	irf: IteratorRef,
	r: ReverseRangeScan,
	done: bool,
}

#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
impl UniqueRangeReverseThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: StoreRangeValue,
		to: StoreRangeValue,
	) -> Result<Self> {
		let r = ReverseRangeScan::new(UniqueRangeThingIterator::range_scan(ns, db, ix, from, to)?);
		Ok(Self {
			irf,
			r,
			done: false,
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<Self> {
		Self::new(irf, ns, db, ix, StoreRangeValue::default(), StoreRangeValue::default())
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<B> {
		if self.done {
			return Ok(B::empty());
		}
		// Check if we include the last record
		let ending_record = if self.r.end_incl {
			// we don't include the ending key for the next batches
			self.r.end_incl = false;
			// tx.scanr is end exclusive, so we have to manually collect the value using a
			// get
			if let Some(v) = tx.get(&self.r.r.end, None).await? {
				let rid: RecordId = revision::from_slice(&v)?;
				let record = IndexItemRecord::new_key(rid, self.irf.into());
				limit -= 1;
				if limit == 0 {
					return Ok(B::from_one(record));
				}
				Some(record)
			} else {
				None
			}
		} else {
			None
		};
		let mut res = tx.scanr(self.r.r.range(), limit, None).await?;
		if let Some((k, _)) = res.last() {
			// We set the ending for the next batch
			self.r.r.end.clone_from(k);
			// If the last key is the beginning of the range, we're done
			if self.r.r.beg.eq(k) {
				self.done = true;
				// Remove the beginning key if it is not supposed to be included
				if !self.r.beg_incl {
					res.remove(res.len() - 1);
				}
			}
		}
		// We collect the records
		let mut records = B::with_capacity(res.len() + ending_record.is_some() as usize);
		if let Some(record) = ending_record {
			records.add(record);
		}
		for (_, v) in res {
			let rid: RecordId = revision::from_slice(&v)?;
			records.add(IndexItemRecord::new_key(rid, self.irf.into()));
		}
		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, mut limit: u32) -> Result<usize> {
		if self.done {
			return Ok(0);
		}
		let mut count = 0;
		// Check if we include the last record
		if self.r.end_incl {
			// we don't include the ending key for the next batches
			self.r.end_incl = false;
			// tx.keysr is end exclusive, so we have to manually check if the value exists
			if tx.exists(&self.r.r.end, None).await? {
				count += 1;
				limit -= 1;
				if limit == 0 {
					return Ok(count);
				}
			}
		}
		let mut res = tx.keysr(self.r.r.range(), limit, None).await?;
		if let Some(k) = res.last() {
			// We set the ending for the next batch
			self.r.r.end.clone_from(k);
			// If the last key is the beginning of the range, we're done
			if self.r.r.beg.eq(k) {
				self.done = true;
				// Remove the beginning if it is not supposed to be included
				if !self.r.beg_incl {
					res.remove(res.len() - 1);
				}
			}
		}
		count += res.len();
		Ok(count)
	}
}

pub(crate) struct UniqueUnionThingIterator {
	irf: IteratorRef,
	keys: VecDeque<Key>,
}

impl UniqueUnionThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fds: &[StoreKeyArray],
	) -> Result<Self> {
		// We create a VecDeque to hold the key for each value in the array.
		let mut keys = VecDeque::with_capacity(fds.len());
		for fd in fds {
			let key = Index::new(ns, db, &ix.what, &ix.name, fd, None).encode_key()?;
			keys.push_back(key);
		}
		Ok(Self {
			irf,
			keys,
		})
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B> {
		let limit = limit as usize;
		let mut results = B::with_capacity(limit.min(self.keys.len()));
		let mut count = 0;
		while let Some(key) = self.keys.pop_front() {
			if ctx.is_done(count % 100 == 0).await? {
				break;
			}
			if let Some(val) = tx.get(&key, None).await? {
				count += 1;
				let rid: RecordId = revision::from_slice(&val)?;
				results.add(IndexItemRecord::new_key(rid, self.irf.into()));
				if results.len() >= limit {
					break;
				}
			}
		}
		Ok(results)
	}

	async fn next_count(&mut self, ctx: &Context, tx: &Transaction, limit: u32) -> Result<usize> {
		let limit = limit as usize;
		let mut count = 0;
		while let Some(key) = self.keys.pop_front() {
			if ctx.is_done(count % 100 == 0).await? {
				break;
			}
			if tx.exists(&key, None).await? {
				count += 1;
				if count >= limit {
					break;
				}
			}
		}
		Ok(count)
	}
}

pub(crate) struct UniqueJoinThingIterator(IteratorRef, JoinThingIterator);

impl UniqueJoinThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: IndexReference,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Result<Self> {
		Ok(Self(irf, JoinThingIterator::new(ns, db, ix, remote_iterators)?))
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B> {
		let new_iter =
			|ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition, value: StoreKeyValue| {
				let array = StoreKeyArray::from(value);
				let it = UniqueEqualThingIterator::new(self.0, ns, db, ix, &array)?;
				Ok(ThingIterator::UniqueEqual(it))
			};
		self.1.next_batch(ctx, tx, limit, new_iter).await
	}

	async fn next_count(&mut self, ctx: &Context, tx: &Transaction, limit: u32) -> Result<usize> {
		let new_iter =
			|ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition, value: StoreKeyValue| {
				let array = StoreKeyArray::from(value);
				let it = UniqueEqualThingIterator::new(self.0, ns, db, ix, &array)?;
				Ok(ThingIterator::UniqueEqual(it))
			};
		self.1.next_count(ctx, tx, limit, new_iter).await
	}
}

pub(crate) trait MatchesHitsIterator {
	fn len(&self) -> usize;
	async fn next(&mut self, tx: &Transaction) -> Result<Option<(RecordId, DocId)>>;
}

pub(crate) struct MatchesThingIterator<T>
where
	T: MatchesHitsIterator,
{
	irf: IteratorRef,
	hits_left: usize,
	hits: Option<T>,
}

impl<T> MatchesThingIterator<T>
where
	T: MatchesHitsIterator,
{
	pub(super) fn new(irf: IteratorRef, hits: Option<T>) -> Self {
		let hits_left = hits.as_ref().map(|h| h.len()).unwrap_or(0);
		Self {
			irf,
			hits,
			hits_left,
		}
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B> {
		if let Some(hits) = &mut self.hits {
			let limit = limit as usize;
			let mut records = B::with_capacity(limit.min(self.hits_left));
			while limit > records.len() {
				if ctx.is_done(self.hits_left % 100 == 0).await? {
					break;
				}
				if let Some((thg, doc_id)) = hits.next(tx).await? {
					let ir = IteratorRecord {
						irf: self.irf,
						doc_id: Some(doc_id),
						dist: None,
					};
					records.add(IndexItemRecord::new_key(thg, ir));
					self.hits_left -= 1;
				} else {
					break;
				}
			}
			Ok(records)
		} else {
			Ok(B::empty())
		}
	}

	async fn next_count(&mut self, ctx: &Context, tx: &Transaction, limit: u32) -> Result<usize> {
		if let Some(hits) = &mut self.hits {
			let limit = limit as usize;
			let mut count = 0;
			while limit > count {
				if ctx.is_done(self.hits_left % 100 == 0).await? {
					break;
				}
				if let Some((_, _)) = hits.next(tx).await? {
					count += 1;
					self.hits_left -= 1;
				} else {
					break;
				}
			}
			Ok(count)
		} else {
			Ok(0)
		}
	}
}

pub(crate) type KnnIteratorResult = (Arc<RecordId>, f64, Option<Arc<Record>>);

pub(crate) struct KnnIterator {
	irf: IteratorRef,
	res: VecDeque<KnnIteratorResult>,
}

impl KnnIterator {
	pub(super) fn new(irf: IteratorRef, res: VecDeque<KnnIteratorResult>) -> Self {
		Self {
			irf,
			res,
		}
	}
	async fn next_batch<B: IteratorBatch>(&mut self, ctx: &Context, limit: u32) -> Result<B> {
		let limit = limit as usize;
		let mut records = B::with_capacity(limit.min(self.res.len()));
		while limit > records.len() {
			if ctx.is_done(records.len() % 100 == 0).await? {
				break;
			}
			if let Some((thing, dist, val)) = self.res.pop_front() {
				let ir = IteratorRecord {
					irf: self.irf,
					doc_id: None,
					dist: Some(dist),
				};
				records.add(IndexItemRecord::new(thing, ir, val));
			} else {
				break;
			}
		}
		Ok(records)
	}

	async fn next_count(&mut self, ctx: &Context, limit: u32) -> Result<usize> {
		let limit = limit as usize;
		let mut count = 0;
		while limit > count {
			if ctx.is_done(count % 100 == 0).await? {
				break;
			}
			if self.res.pop_front().is_some() {
				count += 1;
			} else {
				break;
			}
		}
		Ok(count)
	}
}
