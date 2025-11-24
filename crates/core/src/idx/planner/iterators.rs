use crate::cnf::COUNT_BATCH_SIZE;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::{FtIndex, HitsIterator};
use crate::idx::planner::plan::RangeValue;
use crate::idx::planner::tree::IndexReference;
use crate::key::index::iu::IndexCountKey;
use crate::key::index::Index;
use crate::key::root::ic::IndexCompactionKey;
use crate::kvs::{Key, Val};
use crate::kvs::{KeyEncode, Transaction};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Ident, Number, Operator, Thing, Value};
use radix_trie::Trie;
use rust_decimal::Decimal;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

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

#[allow(clippy::large_enum_variant)]
pub(crate) enum ThingIterator {
	IndexEqual(IndexEqualThingIterator),
	IndexRange(IndexRangeThingIterator),
	#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
	IndexRangeReverse(IndexRangeReverseThingIterator),
	IndexUnion(IndexUnionThingIterator),
	IndexJoin(Box<IndexJoinThingIterator>),
	IndexCount(IndexCountThingIterator),
	UniqueEqual(UniqueEqualThingIterator),
	UniqueRange(UniqueRangeThingIterator),
	#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
	UniqueRangeReverse(UniqueRangeReverseThingIterator),
	UniqueUnion(UniqueUnionThingIterator),
	UniqueJoin(Box<UniqueJoinThingIterator>),
	Matches(MatchesThingIterator),
	Knn(KnnIterator),
	Multiples(Box<MultipleIterators>),
}

impl ThingIterator {
	pub(crate) async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		size: u32,
	) -> Result<B, Error> {
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
			Self::Matches(i) => i.next_batch(ctx, txn, size).await,
			Self::Knn(i) => i.next_batch(ctx, size).await,
			Self::IndexJoin(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
			Self::UniqueJoin(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
			Self::Multiples(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
			Self::IndexCount(_) => {
				Err(Error::unreachable("IndexCount should not be used with next_batch"))
			}
		}
	}

	pub(crate) async fn next_count(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		size: u32,
	) -> Result<usize, Error> {
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
			Self::Matches(i) => i.next_count(ctx, txn, size).await,
			Self::Knn(i) => i.next_count(ctx, size).await,
			Self::IndexJoin(i) => Box::pin(i.next_count(ctx, txn, size)).await,
			Self::UniqueJoin(i) => Box::pin(i.next_count(ctx, txn, size)).await,
			Self::Multiples(i) => Box::pin(i.next_count(ctx, txn, size)).await,
			Self::IndexCount(i) => i.next_count(ctx, txn, size).await,
		}
	}
}

pub(crate) enum IndexItemRecord {
	/// We just collected the key
	Key(Arc<Thing>, IteratorRecord),
	/// We have collected the key and the value
	KeyValue(Arc<Thing>, Arc<Value>, IteratorRecord),
}

impl IndexItemRecord {
	fn new(t: Arc<Thing>, ir: IteratorRecord, val: Option<Arc<Value>>) -> Self {
		if let Some(val) = val {
			Self::KeyValue(t, val, ir)
		} else {
			Self::Key(t, ir)
		}
	}

	fn new_key(t: Thing, ir: IteratorRecord) -> Self {
		Self::Key(Arc::new(t), ir)
	}
	fn thing(&self) -> &Thing {
		match self {
			Self::Key(t, _) => t,
			Self::KeyValue(t, _, _) => t,
		}
	}

	pub(crate) fn consume(self) -> (Arc<Thing>, Option<Arc<Value>>, IteratorRecord) {
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
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		a: &Array,
	) -> Result<Self, Error> {
		let (beg, end) = Self::get_beg_end(ns, db, ix, a)?;
		Ok(Self {
			irf,
			beg,
			end,
		})
	}

	fn get_beg_end(
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		a: &Array,
	) -> Result<(Vec<u8>, Vec<u8>), Error> {
		Ok(if ix.cols.len() == 1 {
			(
				Index::prefix_ids_beg(ns, db, &ix.what, &ix.name, a)?,
				Index::prefix_ids_end(ns, db, &ix.what, &ix.name, a)?,
			)
		} else {
			(
				Index::prefix_ids_composite_beg(ns, db, &ix.what, &ix.name, a)?,
				Index::prefix_ids_composite_end(ns, db, &ix.what, &ix.name, a)?,
			)
		})
	}

	async fn next_scan(
		tx: &Transaction,
		beg: &mut Vec<u8>,
		end: &[u8],
		limit: u32,
	) -> Result<Vec<(Key, Val)>, Error> {
		let min = beg.clone();
		let max = end.to_owned();
		let res = tx.scan(min..max, limit, None).await?;
		if let Some((key, _)) = res.last() {
			let mut key = key.clone();
			key.push(0x00);
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
	) -> Result<B, Error> {
		let res = Self::next_scan(tx, beg, end, limit).await?;
		let mut records = B::with_capacity(res.len());
		res.into_iter().try_for_each(|(_, val)| -> Result<(), Error> {
			records.add(IndexItemRecord::new_key(revision::from_slice(&val)?, irf.into()));
			Ok(())
		})?;
		Ok(records)
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		Self::next_scan_batch(tx, self.irf, &mut self.beg, &self.end, limit).await
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize, Error> {
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

	fn matches(&mut self, k: &Key) -> bool {
		// We check if we should match the key matching the beginning of the range
		if !self.beg_excl_match_checked && self.beg.eq(k) {
			self.beg_excl_match_checked = true;
			return false;
		}
		// We check if we should match the key matching the end of the range
		if !self.end_excl_match_checked && self.end.eq(k) {
			self.end_excl_match_checked = true;
			return false;
		}
		true
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

pub(super) struct IteratorRange<'a> {
	value_type: ValueType,
	from: Cow<'a, RangeValue>,
	to: Cow<'a, RangeValue>,
}

impl<'a> IteratorRange<'a> {
	pub(super) fn new(t: ValueType, from: RangeValue, to: RangeValue) -> Self {
		IteratorRange {
			value_type: t,
			from: Cow::Owned(from),
			to: Cow::Owned(to),
		}
	}

	pub(super) fn new_ref(t: ValueType, from: &'a RangeValue, to: &'a RangeValue) -> Self {
		IteratorRange {
			value_type: t,
			from: Cow::Borrowed(from),
			to: Cow::Borrowed(to),
		}
	}
}

// When we know the type of the range values, we have the opportunity
// to restrict the key range to the exact prefixes according to the type.
#[derive(Copy, Clone)]
pub(super) enum ValueType {
	None,
	NumberInt,
	NumberFloat,
	NumberDecimal,
}

impl ValueType {
	fn prefix_beg(
		&self,
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
	) -> Result<Vec<u8>, Error> {
		match self {
			Self::None => Index::prefix_beg(ns, db, ix_what, ix_name),
			Self::NumberInt => Index::prefix_ids_beg(
				ns,
				db,
				ix_what,
				ix_name,
				&Array(vec![Value::Number(Number::Int(i64::MIN))]),
			),
			Self::NumberFloat => Index::prefix_ids_beg(
				ns,
				db,
				ix_what,
				ix_name,
				&Array(vec![Value::Number(Number::Float(f64::MIN))]),
			),
			Self::NumberDecimal => Index::prefix_ids_beg(
				ns,
				db,
				ix_what,
				ix_name,
				&Array(vec![Value::Number(Number::Decimal(Decimal::MIN))]),
			),
		}
	}

	fn prefix_end(
		&self,
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
	) -> Result<Vec<u8>, Error> {
		match self {
			Self::None => Index::prefix_end(ns, db, ix_what, ix_name),
			Self::NumberInt => Index::prefix_ids_end(
				ns,
				db,
				ix_what,
				ix_name,
				&Array(vec![Value::Number(Number::Int(i64::MAX))]),
			),
			Self::NumberFloat => Index::prefix_ids_end(
				ns,
				db,
				ix_what,
				ix_name,
				&Array(vec![Value::Number(Number::Float(f64::MAX))]),
			),
			Self::NumberDecimal => Index::prefix_ids_end(
				ns,
				db,
				ix_what,
				ix_name,
				&Array(vec![Value::Number(Number::Decimal(Decimal::MAX))]),
			),
		}
	}
}

pub(crate) struct IndexRangeThingIterator {
	irf: IteratorRef,
	r: RangeScan,
}

impl IndexRangeThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		range: &IteratorRange<'_>,
	) -> Result<Self, Error> {
		Ok(Self {
			irf,
			r: Self::range_scan(ns, db, ix, range)?,
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
	) -> Result<Self, Error> {
		let range = full_iterator_range();
		Self::new(irf, ns, db, ix, &range)
	}

	pub(super) fn compound_range(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &IndexReference,
		prefix: &[Value],
		ranges: &[(Operator, Arc<Value>)],
	) -> Result<Self, Error> {
		let (from, to) = Self::reduce_range(ranges)?;
		Ok(Self {
			irf,
			r: Self::range_scan_prefix(ns, db, ix, prefix, &from, &to)?,
		})
	}

	/// Determines the lowest and highest values in the range
	fn reduce_range(ranges: &[(Operator, Arc<Value>)]) -> Result<(RangeValue, RangeValue), Error> {
		let mut from = vec![];
		let mut to = vec![];
		for (op, v) in ranges {
			let v = v.clone();
			let key = storekey::serialize(&v)?;
			match op {
				Operator::LessThan => to.push((key, false, v)),
				Operator::LessThanOrEqual => to.push((key, true, v.clone())),
				Operator::MoreThan => from.push((key, true, v.clone())),
				Operator::MoreThanOrEqual => from.push((key, false, v.clone())),
				_ => {
					return Err(Error::Unreachable(format!(
						"Invalid operator for range extraction {op}"
					)))
				}
			}
		}
		// Sort candidates by encoded key. For lower bounds we want the greatest value (max),
		// for upper bounds we want the smallest (min). The comparator orders by key descending
		// (b1.cmp(a1)), and for equal keys orders by the boolean flag so that strict operators
		// take precedence when choosing the tightest bound.
		let cmp =
			|(a1, a2, _): &(Vec<u8>, bool, Arc<Value>),
			 (b1, b2, _): &(Vec<u8>, bool, Arc<Value>)| { b1.cmp(a1).then_with(|| b2.cmp(a2)) };
		from.sort_unstable_by(cmp);
		to.sort_unstable_by(cmp);
		// Pick the strongest lower bound: first element after sorting (greatest key).
		// The stored boolean reflects the original operator kind: true for strict (>, <),
		// false for inclusive (>=, <=). For the final bound, inclusive is the inverse for
		// lower bounds because a strict '>' becomes an exclusive range start.
		let from = if let Some((_, inclusivity, val)) = from.into_iter().next() {
			RangeValue {
				value: val,
				inclusive: !inclusivity,
			}
		} else {
			RangeValue::default()
		};
		// Pick the strongest upper bound: last element after sorting (smallest key).
		// Here the inclusive flag matches the operator: '<=' is inclusive, '<' is exclusive.
		let to = if let Some((_, inclusivity, val)) = to.into_iter().next_back() {
			RangeValue {
				value: val,
				inclusive: inclusivity,
			}
		} else {
			RangeValue::default()
		};
		Ok((from, to))
	}

	fn range_scan(
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		range: &IteratorRange<'_>,
	) -> Result<RangeScan, Error> {
		let beg = Self::compute_beg(ns, db, &ix.what, &ix.name, &range.from, range.value_type)?;
		let end = Self::compute_end(ns, db, &ix.what, &ix.name, &range.to, range.value_type)?;
		Ok(RangeScan::new(beg, range.from.inclusive, end, range.to.inclusive))
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
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		from: &RangeValue,
		value_type: ValueType,
	) -> Result<Vec<u8>, Error> {
		if from.value.is_none() {
			return value_type.prefix_beg(ns, db, ix_what, ix_name);
		}
		let fd = Array::from(from.value.as_ref().clone());
		if from.inclusive {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &fd)
		} else {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &fd)
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
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		to: &RangeValue,
		value_type: ValueType,
	) -> Result<Vec<u8>, Error> {
		if to.value.as_ref().is_none() {
			return value_type.prefix_end(ns, db, ix_what, ix_name);
		}
		let fd = Array::from(to.value.as_ref().clone());
		if to.inclusive {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &fd)
		} else {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &fd)
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
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		prefix: &[Value],
		from: &RangeValue,
		to: &RangeValue,
	) -> Result<RangeScan, Error> {
		// Prepare the fixed composite prefix (may be empty for the leading column)
		let prefix_array = if prefix.is_empty() {
			Array(Vec::with_capacity(1))
		} else {
			Array::from(prefix.to_vec())
		};
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
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		prefix: &Array,
		from: &RangeValue,
	) -> Result<Vec<u8>, Error> {
		let mut fd = prefix.clone();
		fd.0.push(from.value.as_ref().clone());
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
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		prefix: &Array,
		to: &RangeValue,
	) -> Result<Vec<u8>, Error> {
		let mut fd = prefix.clone();
		fd.0.push(to.value.as_ref().clone());
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
	async fn next_scan(&mut self, tx: &Transaction, limit: u32) -> Result<Vec<(Key, Val)>, Error> {
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
	async fn next_keys(&mut self, tx: &Transaction, limit: u32) -> Result<Vec<Key>, Error> {
		let res = tx.keys(self.r.range(), limit, None).await?;
		if let Some(key) = res.last() {
			self.r.beg.clone_from(key);
			// Same pagination technique as in next_scan: move begin strictly past
			// the last seen key so subsequent calls don't re-count it.
			self.r.beg.push(0x00);
		}
		Ok(res)
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		let res = self.next_scan(tx, limit).await?;
		let mut records = B::with_capacity(res.len());
		res.into_iter().filter(|(k, _)| self.r.matches(k)).try_for_each(
			|(_, v)| -> Result<(), Error> {
				records.add(IndexItemRecord::new_key(revision::from_slice(&v)?, self.irf.into()));
				Ok(())
			},
		)?;
		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize, Error> {
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
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		range: &IteratorRange<'_>,
	) -> Result<Self, Error> {
		Ok(Self {
			irf,
			r: ReverseRangeScan::new(IndexRangeThingIterator::range_scan(ns, db, ix, range)?),
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
	) -> Result<Self, Error> {
		let range = full_iterator_range();
		Self::new(irf, ns, db, ix, &range)
	}
	async fn check_batch_ending(
		&mut self,
		tx: &Transaction,
		limit: &mut u32,
	) -> Result<Option<IndexItemRecord>, Error> {
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

	async fn check_keys_ending(
		&mut self,
		tx: &Transaction,
		limit: &mut u32,
	) -> Result<bool, Error> {
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
	) -> Result<B, Error> {
		// Check if we need to retrieve the key at end of the range (not returned by the scanr)
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
			|(_, v)| -> Result<(), Error> {
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

	async fn next_count(&mut self, tx: &Transaction, mut limit: u32) -> Result<usize, Error> {
		// Check if we need to retrieve the key at end of the range (not returned by the keysr)
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
			self.r.r.end = key.clone();
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
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		arrays: &[Array],
	) -> Result<Self, Error> {
		// We create a VecDeque to hold the prefix keys (begin and end) for each value in the array.
		let mut values: VecDeque<(Vec<u8>, Vec<u8>)> = VecDeque::with_capacity(arrays.len());

		for a in arrays {
			let (beg, end) = IndexEqualThingIterator::get_beg_end(ns, db, ix, a)?;
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
	) -> Result<B, Error> {
		while let Some(r) = &mut self.current {
			if ctx.is_done(true)? {
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

	async fn next_count(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize, Error> {
		while let Some(r) = &mut self.current {
			if ctx.is_done(true)? {
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
	ns: String,
	db: String,
	ix: IndexReference,
	remote_iterators: VecDeque<ThingIterator>,
	current_remote: Option<ThingIterator>,
	current_remote_batch: VecDeque<IndexItemRecord>,
	current_local: Option<ThingIterator>,
	distinct: Trie<Key, bool>,
}

impl JoinThingIterator {
	pub(super) fn new(
		opt: &Options,
		ix: IndexReference,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Result<Self, Error> {
		let (ns, db) = opt.ns_db()?;
		Ok(Self {
			ns: ns.to_owned(),
			db: db.to_owned(),
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
	) -> Result<bool, Error> {
		while !ctx.is_done(true)? {
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
	) -> Result<bool, Error>
	where
		F: Fn(&str, &str, &DefineIndexStatement, Value) -> Result<ThingIterator, Error>,
	{
		while !ctx.is_done(true)? {
			let mut count = 0;
			while let Some(r) = self.current_remote_batch.pop_front() {
				if ctx.is_done(count % 100 == 0)? {
					break;
				}
				let thing = r.thing();
				let k: Key = revision::to_vec(thing)?;
				let value = Value::from(thing.clone());
				if self.distinct.insert(k, true).is_none() {
					self.current_local = Some(new_iter(&self.ns, &self.db, &self.ix, value)?);
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
	) -> Result<B, Error>
	where
		F: Fn(&str, &str, &DefineIndexStatement, Value) -> Result<ThingIterator, Error> + Copy,
	{
		while !ctx.is_done(true)? {
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
	) -> Result<usize, Error>
	where
		F: Fn(&str, &str, &DefineIndexStatement, Value) -> Result<ThingIterator, Error> + Copy,
	{
		while !ctx.is_done(true)? {
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
		opt: &Options,
		ix: IndexReference,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Result<Self, Error> {
		Ok(Self(irf, JoinThingIterator::new(opt, ix, remote_iterators)?))
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		let new_iter = |ns: &str, db: &str, ix: &DefineIndexStatement, value: Value| {
			let array = Array::from(value);
			let it = IndexEqualThingIterator::new(self.0, ns, db, ix, &array)?;
			Ok(ThingIterator::IndexEqual(it))
		};
		self.1.next_batch(ctx, tx, limit, new_iter).await
	}

	async fn next_count(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize, Error> {
		let new_iter = |ns: &str, db: &str, ix: &DefineIndexStatement, value: Value| {
			let array = Array::from(value);
			let it = IndexEqualThingIterator::new(self.0, ns, db, ix, &array)?;
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
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		a: &Array,
	) -> Result<Self, Error> {
		let key = Index::new(ns, db, &ix.what, &ix.name, a, None).encode()?;
		Ok(Self {
			irf,
			key: Some(key),
		})
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction) -> Result<B, Error> {
		if let Some(key) = self.key.take() {
			if let Some(val) = tx.get(key, None).await? {
				let rid: Thing = revision::from_slice(&val)?;
				let record = IndexItemRecord::new_key(rid, self.irf.into());
				return Ok(B::from_one(record));
			}
		}
		Ok(B::empty())
	}

	async fn next_count(&mut self, tx: &Transaction) -> Result<usize, Error> {
		if let Some(key) = self.key.take() {
			if tx.exists(key, None).await? {
				return Ok(1);
			}
		}
		Ok(0)
	}
}

fn full_iterator_range<'a>() -> IteratorRange<'a> {
	let value = RangeValue {
		value: Arc::new(Value::None),
		inclusive: true,
	};
	IteratorRange {
		value_type: ValueType::None,
		from: Cow::Owned(value.clone()),
		to: Cow::Owned(value),
	}
}

pub(crate) struct UniqueRangeThingIterator {
	irf: IteratorRef,
	r: RangeScan,
	done: bool,
}

impl UniqueRangeThingIterator {
	fn range_scan(
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		range: &IteratorRange<'_>,
	) -> Result<RangeScan, Error> {
		let beg = Self::compute_beg(ns, db, &ix.what, &ix.name, &range.from, range.value_type)?;
		let end = Self::compute_end(ns, db, &ix.what, &ix.name, &range.to, range.value_type)?;
		Ok(RangeScan::new(beg, range.from.inclusive, end, range.to.inclusive))
	}

	pub(super) fn new(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
		r: &IteratorRange<'_>,
	) -> Result<Self, Error> {
		let r = Self::range_scan(ns, db, ix, r)?;
		Ok(Self {
			irf,
			r,
			done: false,
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
	) -> Result<Self, Error> {
		let rng = full_iterator_range();
		Self::new(irf, ns, db, ix, &rng)
	}

	pub(super) fn compound_range(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &IndexReference,
		prefix: &[Value],
		ranges: &[(Operator, Arc<Value>)],
	) -> Result<Self, Error> {
		let (from, to) = IndexRangeThingIterator::reduce_range(ranges)?;
		let r = IndexRangeThingIterator::range_scan_prefix(ns, db, ix, prefix, &from, &to)?;
		Ok(Self {
			irf,
			r,
			done: false,
		})
	}
	fn compute_beg(
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		from: &RangeValue,
		value_type: ValueType,
	) -> Result<Vec<u8>, Error> {
		if from.value.is_none() {
			return value_type.prefix_beg(ns, db, ix_what, ix_name);
		}
		Index::new(ns, db, ix_what, ix_name, &Array::from(from.value.as_ref().clone()), None)
			.encode()
	}

	fn compute_end(
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		to: &RangeValue,
		value_type: ValueType,
	) -> Result<Vec<u8>, Error> {
		if to.value.is_none() {
			return value_type.prefix_end(ns, db, ix_what, ix_name);
		}
		Index::new(ns, db, ix_what, ix_name, &Array::from(to.value.as_ref().to_owned()), None)
			.encode()
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<B, Error> {
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
				let rid: Thing = revision::from_slice(&v)?;
				records.add(IndexItemRecord::new_key(rid, self.irf.into()));
			}
		}

		if self.r.matches_end() {
			if let Some(v) = tx.get(&self.r.end, None).await? {
				let rid: Thing = revision::from_slice(&v)?;
				records.add(IndexItemRecord::new_key(rid, self.irf.into()));
			}
		}
		self.done = true;
		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, mut limit: u32) -> Result<usize, Error> {
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
	pub(super) fn full_range(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
	) -> Result<Self, Error> {
		let r = full_iterator_range();
		let r = ReverseRangeScan::new(UniqueRangeThingIterator::range_scan(ns, db, ix, &r)?);
		Ok(Self {
			irf,
			r,
			done: false,
		})
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<B, Error> {
		if self.done {
			return Ok(B::empty());
		}
		// Check if we include the last record
		let ending_record = if self.r.end_incl {
			// we don't include the ending key for the next batches
			self.r.end_incl = false;
			// tx.scanr is end exclusive, so we have to manually collect the value using a get
			if let Some(v) = tx.get(&self.r.r.end, None).await? {
				let rid: Thing = revision::from_slice(&v)?;
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
			self.r.r.end = k.clone();
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
			let rid: Thing = revision::from_slice(&v)?;
			records.add(IndexItemRecord::new_key(rid, self.irf.into()));
		}
		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, mut limit: u32) -> Result<usize, Error> {
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
			self.r.r.end = k.clone();
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
		opt: &Options,
		ix: &DefineIndexStatement,
		vals: &[Array],
	) -> Result<Self, Error> {
		// We create a VecDeque to hold the key for each value in the array.
		let mut keys = VecDeque::with_capacity(vals.len());
		let (ns, db) = opt.ns_db()?;
		for a in vals {
			let key = Index::new(ns, db, &ix.what, &ix.name, a, None).encode()?;
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
	) -> Result<B, Error> {
		let limit = limit as usize;
		let mut results = B::with_capacity(limit.min(self.keys.len()));
		let mut count = 0;
		while let Some(key) = self.keys.pop_front() {
			if ctx.is_done(count % 100 == 0)? {
				break;
			}
			if let Some(val) = tx.get(key, None).await? {
				count += 1;
				let rid: Thing = revision::from_slice(&val)?;
				results.add(IndexItemRecord::new_key(rid, self.irf.into()));
				if results.len() >= limit {
					break;
				}
			}
		}
		Ok(results)
	}

	async fn next_count(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize, Error> {
		let limit = limit as usize;
		let mut count = 0;
		while let Some(key) = self.keys.pop_front() {
			if ctx.is_done(count % 100 == 0)? {
				break;
			}
			if tx.exists(key, None).await? {
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
		opt: &Options,
		ix: IndexReference,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Result<Self, Error> {
		Ok(Self(irf, JoinThingIterator::new(opt, ix, remote_iterators)?))
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		let new_iter = |ns: &str, db: &str, ix: &DefineIndexStatement, value: Value| {
			let array = Array::from(value.clone());
			let it = UniqueEqualThingIterator::new(self.0, ns, db, ix, &array)?;
			Ok(ThingIterator::UniqueEqual(it))
		};
		self.1.next_batch(ctx, tx, limit, new_iter).await
	}

	async fn next_count(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize, Error> {
		let new_iter = |ns: &str, db: &str, ix: &DefineIndexStatement, value: Value| {
			let array = Array::from(value.clone());
			let it = UniqueEqualThingIterator::new(self.0, ns, db, ix, &array)?;
			Ok(ThingIterator::UniqueEqual(it))
		};
		self.1.next_count(ctx, tx, limit, new_iter).await
	}
}

pub(crate) struct MatchesThingIterator {
	irf: IteratorRef,
	hits_left: usize,
	hits: Option<HitsIterator>,
}

impl MatchesThingIterator {
	pub(super) async fn new(
		irf: IteratorRef,
		fti: &FtIndex,
		terms_docs: TermsDocs,
	) -> Result<Self, Error> {
		let hits = fti.new_hits_iterator(terms_docs)?;
		let hits_left = if let Some(h) = &hits {
			h.len()
		} else {
			0
		};
		Ok(Self {
			irf,
			hits,
			hits_left,
		})
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		if let Some(hits) = &mut self.hits {
			let limit = limit as usize;
			let mut records = B::with_capacity(limit.min(self.hits_left));
			while limit > records.len() && !ctx.is_done(self.hits_left % 100 == 0)? {
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

	async fn next_count(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize, Error> {
		if let Some(hits) = &mut self.hits {
			let limit = limit as usize;
			let mut count = 0;
			while limit > count && !ctx.is_done(self.hits_left % 100 == 0)? {
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

pub(crate) type KnnIteratorResult = (Arc<Thing>, f64, Option<Arc<Value>>);

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
	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		limit: u32,
	) -> Result<B, Error> {
		let limit = limit as usize;
		let mut records = B::with_capacity(limit.min(self.res.len()));
		while limit > records.len() && !ctx.is_done(records.len() % 100 == 0)? {
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

	async fn next_count(&mut self, ctx: &Context, limit: u32) -> Result<usize, Error> {
		let limit = limit as usize;
		let mut count = 0;
		while limit > count && !ctx.is_done(count % 100 == 0)? {
			if self.res.pop_front().is_some() {
				count += 1;
			} else {
				break;
			}
		}
		Ok(count)
	}
}

pub(crate) struct MultipleIterators {
	iterators: VecDeque<ThingIterator>,
	current: Option<ThingIterator>,
}

impl MultipleIterators {
	pub(super) fn new(iterators: VecDeque<ThingIterator>) -> Self {
		Self {
			iterators,
			current: None,
		}
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		loop {
			// Do we have an iterator
			if let Some(i) = &mut self.current {
				// If so, take the next batch
				let b: B = i.next_batch(ctx, txn, limit).await?;
				// Return the batch if it is not empty
				if !b.is_empty() {
					return Ok(b);
				}
			}
			// Otherwise check if there is another iterator
			self.current = self.iterators.pop_front();
			if self.current.is_none() {
				// If none, we are done
				return Ok(B::empty());
			}
		}
	}

	async fn next_count(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		limit: u32,
	) -> Result<usize, Error> {
		loop {
			// Do we have an iterator
			if let Some(i) = &mut self.current {
				// If so, take the next batch
				let count = i.next_count(ctx, txn, limit).await?;
				// Return the batch if it is not empty
				if count > 0 {
					return Ok(count);
				}
			}
			// Otherwise check if there is another iterator
			self.current = self.iterators.pop_front();
			if self.current.is_none() {
				// If none, we are done
				return Ok(0);
			}
		}
	}
}

pub(crate) struct IndexCountThingIterator(Option<Range<Key>>);

impl IndexCountThingIterator {
	pub(in crate::idx) fn new(ns: &str, db: &str, tb: &str, ix: &str) -> Result<Self, Error> {
		Ok(Self(Some(IndexCountKey::range(ns, db, tb, ix)?)))
	}
	async fn next_count(
		&mut self,
		ctx: &Context,
		txn: &Transaction,
		_limit: u32,
	) -> Result<usize, Error> {
		if let Some(range) = self.0.take() {
			let mut count: i64 = 0;
			let mut loops = 0;
			let mut current_range = Some(range);
			while let Some(range) = current_range {
				let batch = txn.batch_keys(range, *COUNT_BATCH_SIZE, None).await?;
				for key in batch.result.iter() {
					loops += 1;
					ctx.is_done(loops % 1000 == 0)?;
					let iu = IndexCountKey::decode_key(key)?;
					if iu.pos {
						count += iu.count as i64;
					} else {
						count -= iu.count as i64;
					}
				}
				current_range = batch.next;
				ctx.is_done(true)?;
			}
			Ok(count as usize)
		} else {
			Ok(0)
		}
	}

	pub(in crate::idx) async fn compaction(
		&mut self,
		ic: &IndexCompactionKey<'_>,
		txn: &Transaction,
	) -> Result<(), Error> {
		let Some(range) = self.0.take() else {
			return Ok(());
		};
		let mut count: i64 = 0;
		let mut loops = 0;
		let mut current_range = Some(range.clone());
		while let Some(r) = current_range {
			let batch = txn.batch_keys(r, *COUNT_BATCH_SIZE, None).await?;
			for key in batch.result.iter() {
				loops += 1;
				if loops % 1000 == 0 {
					yield_now!()
				}
				let iu = IndexCountKey::decode_key(key)?;
				if iu.pos {
					count += iu.count as i64;
				} else {
					count -= iu.count as i64;
				}
			}
			current_range = batch.next;
		}
		txn.delr(range).await?;
		let pos = count.is_positive();
		let count = count.unsigned_abs();
		let compact_key = IndexCountKey::new(&ic.ns, &ic.db, &ic.tb, &ic.ix, None, pos, count);
		txn.put(&compact_key, vec![], None).await?;
		Ok(())
	}
}
