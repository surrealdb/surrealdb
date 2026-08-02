use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::Bound;
use std::slice;
use std::sync::Arc;

use ahash::HashSet;
use anyhow::{Result, bail};
use surrealdb_types::ToSql;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId, Record};
use crate::ctx::FrozenContext;
use crate::err::EngineError;
use crate::expr::BinaryOperator;
use crate::idx::count::IndexCountThingIterator;
use crate::idx::docids::DocId;
use crate::idx::entry::IndexEntryValue;
use crate::idx::ft::MatchesHitsIterator;
use crate::idx::ft::fulltext::FullTextHitsIterator;
use crate::idx::planner::tree::IndexReference;
use crate::idx::trees::KnnIteratorResult;
use crate::key::schema::{DbRoot, EntryFdOpenPrefix, EntryFdPrefix, EntryPrefix, UniqueKey};
use crate::key::{AnyRange, KVKey, KVRange, KVSubspace, KeyRange, TypedRange};
use crate::kvs::Transaction;
use crate::kvs::util::{scan, scan_keys, scanr, scanr_keys};
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

	fn add_key(&mut self, id: RecordId, record: IteratorRecord) {
		self.add(Arc::new(id), record, None)
	}

	fn add(&mut self, id: Arc<RecordId>, record: IteratorRecord, f: Option<Arc<Record>>);

	fn len(&self) -> usize;
	fn is_empty(&self) -> bool;
}

impl IteratorBatch for Vec<IndexItemRecord> {
	fn empty() -> Self {
		Vec::new()
	}

	fn with_capacity(capacity: usize) -> Self {
		Vec::with_capacity(capacity)
	}

	fn add(&mut self, id: Arc<RecordId>, record: IteratorRecord, f: Option<Arc<Record>>) {
		self.push(IndexItemRecord::new(id, record, f))
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
		VecDeque::new()
	}
	fn with_capacity(capacity: usize) -> Self {
		VecDeque::with_capacity(capacity)
	}

	fn add(&mut self, id: Arc<RecordId>, record: IteratorRecord, f: Option<Arc<Record>>) {
		self.push_back(IndexItemRecord::new(id, record, f))
	}

	fn len(&self) -> usize {
		VecDeque::len(self)
	}
	fn is_empty(&self) -> bool {
		VecDeque::is_empty(self)
	}
}

impl IteratorBatch for VecDeque<RecordId> {
	fn empty() -> Self {
		VecDeque::new()
	}
	fn with_capacity(capacity: usize) -> Self {
		VecDeque::with_capacity(capacity)
	}

	fn add_key(&mut self, id: RecordId, _: IteratorRecord) {
		self.push_back(id)
	}

	fn add(&mut self, id: Arc<RecordId>, _: IteratorRecord, _: Option<Arc<Record>>) {
		self.push_back((*id).clone())
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
pub(crate) enum RecordIterator {
	IndexEqual(IndexEqualThingIterator),
	IndexRange(IndexRangeThingIterator),
	IndexRangeReverse(IndexRangeReverseThingIterator),
	IndexUnion(IndexUnionThingIterator),
	IndexJoin(Box<IndexJoinThingIterator>),
	IndexCount(IndexCountThingIterator),
	UniqueEqual(UniqueEqualThingIterator),
	UniqueRange(UniqueRangeThingIterator),
	UniqueRangeReverse(UniqueRangeReverseThingIterator),
	UniqueUnion(UniqueUnionThingIterator),
	UniqueJoin(Box<UniqueJoinThingIterator>),
	FullTextMatches(Box<MatchesThingIterator<FullTextHitsIterator>>),
	Knn(KnnIterator),
}

impl RecordIterator {
	/// Fetch the next batch of index items.
	///
	/// - `size` is a soft upper bound on how many items to fetch. Concrete iterators may return
	///   fewer items (e.g., due to range boundaries) or, in rare edge-cases, one extra to honor
	///   inclusivity semantics when scanning in reverse.
	pub(crate) async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &FrozenContext,
		txn: &Transaction,
		size: u32,
	) -> Result<B> {
		match self {
			Self::IndexEqual(i) => i.next_batch(txn, size).await,
			Self::UniqueEqual(i) => i.next_batch(txn, size).await,
			Self::IndexRange(i) => i.next_batch(txn, size).await,
			Self::IndexRangeReverse(i) => i.next_batch(txn, size).await,
			Self::UniqueRange(i) => i.next_batch(txn, size).await,
			Self::UniqueRangeReverse(i) => i.next_batch(txn, size).await,
			Self::IndexUnion(i) => i.next_batch(ctx, txn, size).await,
			Self::UniqueUnion(i) => i.next_batch(ctx, txn, size).await,
			Self::FullTextMatches(i) => i.next_batch(ctx, txn, size).await,
			Self::Knn(i) => i.next_batch(ctx, size).await,
			Self::IndexJoin(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
			Self::UniqueJoin(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
			Self::IndexCount(_) => {
				bail!(EngineError::unreachable("IndexCount should not be used with next_batch"))
			}
		}
	}

	/// Count up to the next `size` matching items without materializing values.
	///
	/// Used for SELECT ... COUNT and for explain paths where only cardinality
	/// is required.
	pub(crate) async fn next_count(
		&mut self,
		ctx: &FrozenContext,
		txn: &Transaction,
		size: u32,
	) -> Result<usize> {
		match self {
			Self::IndexEqual(i) => i.next_count(txn, size).await,
			Self::UniqueEqual(i) => i.next_count(txn, size).await,
			Self::IndexRange(i) => i.next_count(txn, size).await,
			Self::IndexRangeReverse(i) => i.next_count(txn, size).await,
			Self::UniqueRange(i) => i.next_count(txn, size).await,
			Self::UniqueRangeReverse(i) => i.next_count(txn, size).await,
			Self::IndexUnion(i) => i.next_count(ctx, txn, size).await,
			Self::UniqueUnion(i) => i.next_count(ctx, txn, size).await,
			Self::FullTextMatches(i) => i.next_count(ctx, txn, size).await,
			Self::Knn(i) => i.next_count(ctx, size).await,
			Self::IndexJoin(i) => Box::pin(i.next_count(ctx, txn, size)).await,
			Self::UniqueJoin(i) => Box::pin(i.next_count(ctx, txn, size)).await,
			Self::IndexCount(i) => i.next_count(ctx, txn, size).await,
		}
	}
}

/// Iterator output record. Either a key-only result (for index-only scans)
/// or a key+value pair when values are fetched by the current RecordStrategy.
#[derive(Debug)]
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

	pub(crate) fn consume(self) -> (Arc<RecordId>, Option<Arc<Record>>, IteratorRecord) {
		match self {
			Self::Key(t, ir) => (t, None, ir),
			Self::KeyValue(t, v, ir) => (t, Some(v), ir),
		}
	}
}

pub(crate) struct IndexEqualThingIterator {
	irf: IteratorRef,
	range: TypedRange<IndexEntryValue>,
}

impl IndexEqualThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fd: &Array,
	) -> Result<Self> {
		let range = Self::get_beg_end(ns, db, ix, fd)?;
		Ok(Self {
			irf,
			range,
		})
	}

	/// Computes the range to scan for an equality lookup on an index.
	///
	/// For single-column indexes, uses simple prefix key generation.
	/// For composite indexes (multiple columns), uses composite key generation
	/// which handles the ordering and encoding of multiple index values.
	///
	/// The returned range covers every entry that exactly matches the provided
	/// array values.
	fn get_beg_end(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fd: &[Value],
	) -> Result<TypedRange<IndexEntryValue>> {
		if ix.cols.len() == 1 {
			// Single column index: straightforward key prefix generation
			EntryFdPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(fd),
			}
			.range()
		} else {
			EntryFdOpenPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(fd),
			}
			.range()
		}
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction, limit: u32) -> Result<B> {
		let res = scan(&mut self.range, tx, limit).await?;
		let mut records = B::with_capacity(res.len());
		for (_, v) in res {
			records.add_key(revision::from_slice(&v)?, self.irf.into());
		}
		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize> {
		Ok(scan_keys(&mut self.range, tx, limit).await?.len())
	}
}

pub(crate) struct IndexRangeThingIterator {
	irf: IteratorRef,
	r: TypedRange<IndexEntryValue>,
}

impl IndexRangeThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
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
		Self::new(irf, ns, db, ix, Bound::Unbounded, Bound::Unbounded)
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
	) -> Result<(Bound<&Value>, Bound<&Value>)> {
		// Returns the bound which has the higher value of the two,
		// Assuming Unbounded is the minimum value
		fn constrain_up<'a>(start: Bound<&'a Value>, cmp: Bound<&'a Value>) -> Bound<&'a Value> {
			match start {
				Bound::Unbounded => cmp,
				Bound::Included(a) => match cmp {
					Bound::Included(b) => Bound::Included(a.max(b)),
					Bound::Excluded(b) => {
						if a <= b {
							Bound::Excluded(b)
						} else {
							Bound::Included(a)
						}
					}
					Bound::Unbounded => Bound::Included(a),
				},
				Bound::Excluded(a) => match cmp {
					Bound::Excluded(b) => Bound::Excluded(a.max(b)),
					Bound::Included(b) => {
						if a < b {
							Bound::Included(b)
						} else {
							Bound::Excluded(a)
						}
					}
					Bound::Unbounded => Bound::Included(a),
				},
			}
		}

		// Returns the bound which has the lower value of the two,
		// Assuming Unbounded is the maximum value
		fn constrain_down<'a>(start: Bound<&'a Value>, cmp: Bound<&'a Value>) -> Bound<&'a Value> {
			match start {
				Bound::Unbounded => cmp,
				Bound::Included(a) => match cmp {
					Bound::Included(b) => Bound::Included(a.min(b)),
					Bound::Excluded(b) => {
						if a >= b {
							Bound::Excluded(b)
						} else {
							Bound::Included(a)
						}
					}
					Bound::Unbounded => Bound::Included(a),
				},
				Bound::Excluded(a) => match cmp {
					Bound::Excluded(b) => Bound::Excluded(a.min(b)),
					Bound::Included(b) => {
						if a > b {
							Bound::Included(b)
						} else {
							Bound::Excluded(a)
						}
					}
					Bound::Unbounded => Bound::Included(a),
				},
			}
		}

		let mut start = Bound::Unbounded;
		let mut end = Bound::Unbounded;

		for (op, v) in ranges {
			match op {
				BinaryOperator::LessThan => {
					end = constrain_down(end, Bound::Excluded(v));
				}
				BinaryOperator::LessThanEqual => {
					end = constrain_down(end, Bound::Included(v));
				}
				BinaryOperator::MoreThan => {
					start = constrain_up(start, Bound::Excluded(v));
				}
				BinaryOperator::MoreThanEqual => {
					start = constrain_up(start, Bound::Included(v));
				}
				_ => {
					bail!(EngineError::Unreachable(format!(
						"Invalid operator for range extraction {}",
						op.to_sql()
					)))
				}
			}
		}
		Ok((start, end))
	}

	fn range_scan(
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<TypedRange<IndexEntryValue>> {
		crate::idx::keys::compute_index_range(ns, db, ix, from, to)
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
		value_prefix: &[Value],
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<TypedRange<IndexEntryValue>> {
		let unterminated_bound = |value_prefix: &[Value]| {
			EntryFdOpenPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(value_prefix),
			}
			.encode_bound()
		};

		let terminated_bound = |value_prefix: &[Value]| {
			EntryFdPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(value_prefix),
			}
			.encode_bound()
		};

		let start = match from {
			Bound::Included(v) => {
				let mut value_prefix = value_prefix.to_vec();
				value_prefix.push(v.clone());
				unterminated_bound(&value_prefix)?
			}
			Bound::Excluded(v) => {
				let mut value_prefix = value_prefix.to_vec();
				value_prefix.push(v.clone());
				unterminated_bound(&value_prefix)?.next_neighbour_expect()
			}
			Bound::Unbounded => unterminated_bound(value_prefix)?,
		};

		let end = match to {
			Bound::Included(v) => {
				let mut value_prefix = value_prefix.to_vec();
				value_prefix.push(v.clone());
				// Needs to be next_neighbour, instead of next, as the actually searched for keys
				// have a trailing suffix after created key.
				terminated_bound(&value_prefix)?.next_neighbour_expect()
			}
			Bound::Excluded(v) => {
				let mut value_prefix = value_prefix.to_vec();
				value_prefix.push(v.clone());
				terminated_bound(&value_prefix)?
			}
			Bound::Unbounded => unterminated_bound(value_prefix)?.next_neighbour_expect(),
		};

		// The boundaries narrow the open bound over `value_prefix`, which is what the
		// range is a slice of.
		Ok(EntryFdOpenPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
			fd: Cow::Borrowed(value_prefix),
		}
		.typed(KeyRange {
			start,
			end,
		}))
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction, limit: u32) -> Result<B> {
		let res = scan(&mut self.r, tx, limit).await?;
		let mut records = B::with_capacity(res.len());
		for (_, v) in res {
			records.add_key(revision::from_slice(&v)?, self.irf.into());
		}

		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize> {
		Ok(scan_keys(&mut self.r, tx, limit).await?.len())
	}
}

pub(crate) struct IndexRangeReverseThingIterator {
	irf: IteratorRef,
	r: TypedRange<IndexEntryValue>,
}

impl IndexRangeReverseThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		Ok(Self {
			irf,
			r: IndexRangeThingIterator::range_scan(ns, db, ix, from, to)?,
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<Self> {
		Self::new(irf, ns, db, ix, Bound::Unbounded, Bound::Unbounded)
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction, limit: u32) -> Result<B> {
		// Do we have enough limit left to collect additional records?
		let scan = scanr(&mut self.r, tx, limit).await?;

		let mut res = B::empty();
		for (_, v) in scan {
			res.add_key(revision::from_slice(&v)?, self.irf.into());
		}
		Ok(res)
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize> {
		Ok(scanr_keys(&mut self.r, tx, limit).await?.len())
	}
}

pub(crate) struct IndexUnionThingIterator {
	irf: IteratorRef,
	/// One range per value, each narrowed to what is left of its scan.
	ranges: Vec<TypedRange<IndexEntryValue>>,
}

impl IndexUnionThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fds: &[Array],
	) -> Result<Self> {
		// We create a VecDeque to hold the prefix keys (begin and end) for each value
		// in the array.
		let mut values = Vec::with_capacity(fds.len());

		for fd in fds {
			let range = IndexEqualThingIterator::get_beg_end(ns, db, ix, fd)?;
			values.push(range);
		}

		values.reverse();

		Ok(Self {
			irf,
			ranges: values,
		})
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<B> {
		let mut res = B::empty();

		while !ctx.is_done(Some(res.len())).await? {
			let Some(last) = self.ranges.last_mut() else {
				break;
			};

			let s = scan(last, tx, limit).await?;

			limit -= s.len() as u32;

			for (_, v) in s {
				res.add_key(revision::from_slice(&v)?, self.irf.into());
			}

			// The scan narrows the range to nothing once it has drained it.
			if last.clone().into_key_range().is_empty() {
				self.ranges.pop();
			}

			if limit == 0 {
				break;
			}
		}

		Ok(res)
	}

	async fn next_count(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<usize> {
		let mut count = 0;

		while !ctx.is_done(Some(count)).await? {
			let Some(last) = self.ranges.last_mut() else {
				return Ok(count);
			};

			let s = scan_keys(last, tx, limit).await?;

			limit -= s.len() as u32;

			count += s.len();

			// The scan narrows the range to nothing once it has drained it.
			if last.clone().into_key_range().is_empty() {
				self.ranges.pop();
			}

			if limit == 0 {
				break;
			}
		}

		Ok(count)
	}
}

struct JoinThingIterator {
	ns: NamespaceId,
	db: DatabaseId,
	ix: IndexReference,
	remote_iterators: VecDeque<RecordIterator>,
	current_remote_batch: VecDeque<RecordId>,
	current_local: Option<RecordIterator>,
	distinct: HashSet<RecordId>,
}

impl JoinThingIterator {
	pub(super) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		ix: IndexReference,
		remote_iterators: VecDeque<RecordIterator>,
	) -> Result<Self> {
		Ok(Self {
			ns,
			db,
			ix,
			current_remote_batch: VecDeque::new(),
			remote_iterators,
			current_local: None,
			distinct: Default::default(),
		})
	}
}

impl JoinThingIterator {
	async fn current_iterator<F>(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
		new_iter: F,
	) -> Result<Option<&mut RecordIterator>>
	where
		F: Fn(NamespaceId, DatabaseId, &IndexDefinition, Value) -> Result<RecordIterator> + Copy,
	{
		let mut count = 0;
		while self.current_local.is_none() && !ctx.is_done(Some(count)).await? {
			count += 1;

			if let Some(r) = self.current_remote_batch.pop_front() {
				if self.distinct.insert(r.clone()) {
					self.current_local =
						Some(new_iter(self.ns, self.db, &self.ix, Value::from(r))?);
					break;
				}
				continue;
			}

			if let Some(r) = self.remote_iterators.front_mut() {
				let batch: VecDeque<RecordId> = r.next_batch(ctx, tx, limit).await?;
				if !batch.is_empty() {
					self.current_remote_batch = batch;
				} else {
					self.remote_iterators.pop_front();
				}
				continue;
			}

			break;
		}

		Ok(self.current_local.as_mut())
	}

	async fn next_batch<F, B: IteratorBatch>(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
		new_iter: F,
	) -> Result<B>
	where
		F: Fn(NamespaceId, DatabaseId, &IndexDefinition, Value) -> Result<RecordIterator> + Copy,
	{
		while !ctx.is_done(None).await? {
			let Some(x) = self.current_iterator(ctx, tx, limit, new_iter).await? else {
				return Ok(B::empty());
			};

			let res: B = x.next_batch(ctx, tx, limit).await?;
			if res.is_empty() {
				self.current_local = None;
				continue;
			}
			return Ok(res);
		}
		Ok(B::empty())
	}

	async fn next_count<F>(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
		new_iter: F,
	) -> Result<usize>
	where
		F: Fn(NamespaceId, DatabaseId, &IndexDefinition, Value) -> Result<RecordIterator> + Copy,
	{
		while !ctx.is_done(None).await? {
			let Some(x) = self.current_iterator(ctx, tx, limit, new_iter).await? else {
				return Ok(0);
			};

			let res = x.next_count(ctx, tx, limit).await?;
			if res == 0 {
				self.current_local = None;
				continue;
			}
			return Ok(res);
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
		remote_iterators: VecDeque<RecordIterator>,
	) -> Result<Self> {
		Ok(Self(irf, JoinThingIterator::new(ns, db, ix, remote_iterators)?))
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
	) -> Result<B> {
		let new_iter = |ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition, value: Value| {
			let fd = Array::from(vec![value]);
			let it = IndexEqualThingIterator::new(self.0, ns, db, ix, &fd)?;
			Ok(RecordIterator::IndexEqual(it))
		};
		self.1.next_batch(ctx, tx, limit, new_iter).await
	}

	async fn next_count(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize> {
		let new_iter = |ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition, value: Value| {
			let fd = Array::from(vec![value]);
			let it = IndexEqualThingIterator::new(self.0, ns, db, ix, &fd)?;
			Ok(RecordIterator::IndexEqual(it))
		};
		self.1.next_count(ctx, tx, limit, new_iter).await
	}
}

/// Equality lookup on a unique index (legacy planner).
///
/// NONE/NULL tuples are stored with non-unique key format (record-ID
/// suffix), so they require a prefix range scan instead of a point-get.
pub(crate) struct UniqueEqualThingIterator {
	irf: IteratorRef,
	range: TypedRange<IndexEntryValue>,
}

impl UniqueEqualThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		a: &Array,
	) -> Result<Self> {
		let inner = if a.is_any_none_or_null() {
			EntryFdPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(a),
			}
			.range()?
		} else {
			let bound = EntryFdPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(a),
			};
			let key = UniqueKey {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(a),
			}
			.encode_key()?;
			let next = key.as_borrowed().next();
			// The one key the unique tuple maps to, as a slice of the bound over
			// that tuple.
			bound.typed(KeyRange {
				start: key,
				end: next,
			})
		};
		Ok(Self {
			irf,
			range: inner,
		})
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction, limit: u32) -> Result<B> {
		let values = scan(&mut self.range, tx, limit).await?;
		let mut res = B::empty();
		for (_, val) in values {
			let rid: RecordId = revision::from_slice(&val)?;
			res.add_key(rid, self.irf.into());
		}
		Ok(res)
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize> {
		Ok(scan_keys(&mut self.range, tx, limit).await?.len())
	}
}

pub(crate) struct UniqueRangeThingIterator {
	irf: IteratorRef,
	r: TypedRange<IndexEntryValue>,
}

impl UniqueRangeThingIterator {
	fn range_scan(
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
					.encode_bound()?
					.next_neighbour_expect()
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
					.encode_bound()?
					.next_neighbour_expect()
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
			.encode_bound()?
			.next_neighbour_expect(),
		};

		// The boundaries narrow the index's entry bound, which is what the range is
		// a slice of.
		Ok(EntryPrefix {
			ns: prefix.ns,
			db: prefix.db,
			tb: Cow::Borrowed(&ix.table_name),
			ix: ix.index_id,
		}
		.typed(KeyRange {
			start,
			end,
		}))
	}

	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let r = Self::range_scan(ns, db, ix, from, to)?;
		Ok(Self {
			irf,
			r,
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<Self> {
		Self::new(irf, ns, db, ix, Bound::Unbounded, Bound::Unbounded)
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
		})
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction, limit: u32) -> Result<B> {
		let res = scan(&mut self.r, tx, limit).await?;
		let mut records = B::with_capacity(res.len());

		for (_, v) in res {
			records.add_key(revision::from_slice(&v)?, self.irf.into());
		}

		Ok(records)
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize> {
		let res = scan_keys(&mut self.r, tx, limit).await?;
		Ok(res.len())
	}
}

pub(crate) struct UniqueRangeReverseThingIterator {
	irf: IteratorRef,
	r: TypedRange<IndexEntryValue>,
}

impl UniqueRangeReverseThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		from: Bound<&Value>,
		to: Bound<&Value>,
	) -> Result<Self> {
		let r = UniqueRangeThingIterator::range_scan(ns, db, ix, from, to)?;
		Ok(Self {
			irf,
			r,
		})
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<Self> {
		Self::new(irf, ns, db, ix, Bound::Unbounded, Bound::Unbounded)
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction, limit: u32) -> Result<B> {
		let values = scanr(&mut self.r, tx, limit).await?;

		// We collect the records
		let mut res = B::with_capacity(values.len());
		for (_, v) in values {
			let rid: RecordId = revision::from_slice(&v)?;
			res.add_key(rid, self.irf.into());
		}
		Ok(res)
	}

	async fn next_count(&mut self, tx: &Transaction, limit: u32) -> Result<usize> {
		Ok(scanr_keys(&mut self.r, tx, limit).await?.len())
	}
}

pub(crate) struct UniqueUnionThingIterator {
	irf: IteratorRef,
	/// One range per value, each narrowed to what is left of its scan.
	entries: Vec<TypedRange<IndexEntryValue>>,
}

impl UniqueUnionThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		fds: &[Array],
	) -> Result<Self> {
		let mut entries = Vec::with_capacity(fds.len());
		// Iterate in reverse, so that the ranges end up in reverse order and can then be popped in
		// the right order from the vec.
		for fd in fds.iter().rev() {
			let bound = EntryFdPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&ix.table_name),
				ix: ix.index_id,
				fd: Cow::Borrowed(fd),
			};
			if fd.is_any_none_or_null() {
				entries.push(bound.range()?)
			} else {
				let start = UniqueKey {
					ns,
					db,
					tb: Cow::Borrowed(&ix.table_name),
					ix: ix.index_id,
					fd: Cow::Borrowed(fd),
				}
				.encode_key()?;
				let end = start.as_borrowed().next();

				// The one key the unique tuple maps to, as a slice of the bound
				// over that tuple.
				entries.push(bound.typed(KeyRange {
					start,
					end,
				}));
			}
		}
		Ok(Self {
			irf,
			entries,
		})
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<B> {
		let mut results = B::empty();
		let mut count = 0;

		while let Some(next) = self.entries.last_mut() {
			if ctx.is_done(Some(count)).await? {
				break;
			}

			let res = scan(next, tx, limit).await?;

			limit -= res.len() as u32;

			for (_, val) in res {
				count += 1;
				let rid: RecordId = revision::from_slice(&val)?;
				results.add_key(rid, self.irf.into());
			}

			// The scan narrows the range to nothing once it has drained it.
			if next.clone().into_key_range().is_empty() {
				self.entries.pop();
			}

			if limit == 0 {
				break;
			}
		}
		Ok(results)
	}

	async fn next_count(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize> {
		let mut res = 0;
		let mut count = 0;

		while let Some(next) = self.entries.last_mut() {
			if ctx.is_done(Some(count)).await? {
				break;
			}
			count += 1;

			let keys = scan_keys(next, tx, limit - res as u32).await?;

			res += keys.len() as usize;

			// The scan narrows the range to nothing once it has drained it.
			if next.clone().into_key_range().is_empty() {
				self.entries.pop();
			}

			if limit as usize <= res {
				break;
			}
		}
		Ok(res)
	}
}

pub(crate) struct UniqueJoinThingIterator(IteratorRef, JoinThingIterator);

impl UniqueJoinThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: NamespaceId,
		db: DatabaseId,
		ix: IndexReference,
		remote_iterators: VecDeque<RecordIterator>,
	) -> Result<Self> {
		Ok(Self(irf, JoinThingIterator::new(ns, db, ix, remote_iterators)?))
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
	) -> Result<B> {
		let new_iter = |ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition, value: Value| {
			let array = Array::from(vec![value]);
			let it = UniqueEqualThingIterator::new(self.0, ns, db, ix, &array)?;
			Ok(RecordIterator::UniqueEqual(it))
		};
		self.1.next_batch(ctx, tx, limit, new_iter).await
	}

	async fn next_count(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize> {
		let new_iter = |ns: NamespaceId, db: DatabaseId, ix: &IndexDefinition, value: Value| {
			let array = Array::from(vec![value]);
			let it = UniqueEqualThingIterator::new(self.0, ns, db, ix, &array)?;
			Ok(RecordIterator::UniqueEqual(it))
		};
		self.1.next_count(ctx, tx, limit, new_iter).await
	}
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
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
	) -> Result<B> {
		if let Some(hits) = &mut self.hits {
			let limit = limit as usize;
			let mut count = 0;
			let mut records = B::with_capacity(limit.min(self.hits_left));
			while limit > records.len() {
				if ctx.is_done(Some(count)).await? {
					break;
				}
				if let Some((thg, doc_id)) = hits.next(tx).await? {
					let ir = IteratorRecord {
						irf: self.irf,
						doc_id: Some(doc_id),
						dist: None,
					};
					records.add_key(thg, ir);
					self.hits_left -= 1;
				} else {
					break;
				}
				count += 1;
			}
			Ok(records)
		} else {
			Ok(B::empty())
		}
	}

	async fn next_count(
		&mut self,
		ctx: &FrozenContext,
		tx: &Transaction,
		limit: u32,
	) -> Result<usize> {
		if let Some(hits) = &mut self.hits {
			let limit = limit as usize;
			let mut count = 0;
			while limit > count {
				if ctx.is_done(Some(count)).await? {
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
	async fn next_batch<B: IteratorBatch>(&mut self, ctx: &FrozenContext, limit: u32) -> Result<B> {
		let limit = limit as usize;
		let mut count = 0;
		let mut records = B::with_capacity(limit.min(self.res.len()));
		while limit > records.len() {
			if ctx.is_done(Some(count)).await? {
				break;
			}
			if let Some((thing, dist, val)) = self.res.pop_front() {
				let ir = IteratorRecord {
					irf: self.irf,
					doc_id: None,
					dist: Some(dist),
				};
				records.add(thing, ir, val);
			} else {
				break;
			}
			count += 1;
		}
		Ok(records)
	}

	async fn next_count(&mut self, ctx: &FrozenContext, limit: u32) -> Result<usize> {
		let limit = limit as usize;
		let mut count = 0;
		while limit > count {
			if ctx.is_done(Some(count)).await? {
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
