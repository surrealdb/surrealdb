use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::{FtIndex, HitsIterator};
use crate::idx::planner::plan::RangeValue;
use crate::key::index::Index;
use crate::kvs::Key;
use crate::kvs::Transaction;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Ident, Number, Thing, Value};
use radix_trie::Trie;
use rust_decimal::Decimal;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;

pub(crate) type IteratorRef = u16;

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
	fn from_one(record: CollectorRecord) -> Self;
	fn add(&mut self, record: CollectorRecord);
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool;
}

impl IteratorBatch for Vec<CollectorRecord> {
	fn empty() -> Self {
		Vec::from([])
	}

	fn with_capacity(capacity: usize) -> Self {
		Vec::with_capacity(capacity)
	}
	fn from_one(record: CollectorRecord) -> Self {
		Vec::from([record])
	}

	fn add(&mut self, record: CollectorRecord) {
		self.push(record)
	}

	fn len(&self) -> usize {
		Vec::len(self)
	}

	fn is_empty(&self) -> bool {
		Vec::is_empty(self)
	}
}

impl IteratorBatch for VecDeque<CollectorRecord> {
	fn empty() -> Self {
		VecDeque::from([])
	}
	fn with_capacity(capacity: usize) -> Self {
		VecDeque::with_capacity(capacity)
	}
	fn from_one(record: CollectorRecord) -> Self {
		VecDeque::from([record])
	}

	fn add(&mut self, record: CollectorRecord) {
		self.push_back(record)
	}

	fn len(&self) -> usize {
		VecDeque::len(self)
	}
	fn is_empty(&self) -> bool {
		VecDeque::is_empty(self)
	}
}

pub(crate) enum ThingIterator {
	IndexEqual(IndexEqualThingIterator),
	IndexRange(IndexRangeThingIterator),
	IndexUnion(IndexUnionThingIterator),
	IndexJoin(Box<IndexJoinThingIterator>),
	UniqueEqual(UniqueEqualThingIterator),
	UniqueRange(UniqueRangeThingIterator),
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
			Self::UniqueRange(i) => i.next_batch(txn, size).await,
			Self::IndexUnion(i) => i.next_batch(ctx, txn, size).await,
			Self::UniqueUnion(i) => i.next_batch(ctx, txn, size).await,
			Self::Matches(i) => i.next_batch(ctx, txn, size).await,
			Self::Knn(i) => i.next_batch(ctx, size).await,
			Self::IndexJoin(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
			Self::UniqueJoin(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
			Self::Multiples(i) => Box::pin(i.next_batch(ctx, txn, size)).await,
		}
	}
}

pub(crate) type CollectorRecord = (Arc<Thing>, IteratorRecord, Option<Arc<Value>>);

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
		v: &Value,
	) -> Self {
		let a = Array::from(v.clone());
		let (beg, end) = if ix.cols.len() == 1 {
			(
				Index::prefix_ids_beg(ns, db, &ix.what, &ix.name, &a),
				Index::prefix_ids_end(ns, db, &ix.what, &ix.name, &a),
			)
		} else {
			(
				Index::prefix_ids_composite_beg(ns, db, &ix.what, &ix.name, &a),
				Index::prefix_ids_composite_end(ns, db, &ix.what, &ix.name, &a),
			)
		};
		Self {
			irf,
			beg,
			end,
		}
	}

	async fn next_scan<B: IteratorBatch>(
		tx: &Transaction,
		irf: IteratorRef,
		beg: &mut Vec<u8>,
		end: &[u8],
		limit: u32,
	) -> Result<B, Error> {
		let min = beg.clone();
		let max = end.to_owned();
		let res = tx.scan(min..max, limit, None).await?;
		if let Some((key, _)) = res.last() {
			let mut key = key.clone();
			key.push(0x00);
			*beg = key;
		}
		let mut records = B::with_capacity(res.len());
		res.into_iter().for_each(|(_, val)| records.add((Arc::new(val.into()), irf.into(), None)));
		Ok(records)
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		Self::next_scan(tx, self.irf, &mut self.beg, &self.end, limit).await
	}
}

struct RangeScan {
	beg: Vec<u8>,
	end: Vec<u8>,
	beg_excl: Option<Vec<u8>>,
	end_excl: Option<Vec<u8>>,
}

impl RangeScan {
	fn new(beg: Vec<u8>, beg_incl: bool, end: Vec<u8>, end_incl: bool) -> Self {
		let beg_excl = if !beg_incl {
			Some(beg.clone())
		} else {
			None
		};
		let end_excl = if !end_incl {
			Some(end.clone())
		} else {
			None
		};
		Self {
			beg,
			end,
			beg_excl,
			end_excl,
		}
	}

	fn matches(&mut self, k: &Key) -> bool {
		if let Some(b) = &self.beg_excl {
			if b.eq(k) {
				self.beg_excl = None;
				return false;
			}
		}
		if let Some(e) = &self.end_excl {
			if e.eq(k) {
				self.end_excl = None;
				return false;
			}
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
	fn prefix_beg(&self, ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident) -> Vec<u8> {
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

	fn prefix_end(&self, ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident) -> Vec<u8> {
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
		ix_what: &Ident,
		ix_name: &Ident,
		range: &IteratorRange<'_>,
	) -> Self {
		let beg = Self::compute_beg(ns, db, ix_what, ix_name, &range.from, range.value_type);
		let end = Self::compute_end(ns, db, ix_what, ix_name, &range.to, range.value_type);
		Self {
			irf,
			r: RangeScan::new(beg, range.from.inclusive, end, range.to.inclusive),
		}
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
	) -> Self {
		let full_range = RangeValue {
			value: Value::None,
			inclusive: true,
		};
		let range = IteratorRange {
			value_type: ValueType::None,
			from: Cow::Borrowed(&full_range),
			to: Cow::Borrowed(&full_range),
		};
		Self::new(irf, ns, db, ix_what, ix_name, &range)
	}

	fn compute_beg(
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		from: &RangeValue,
		value_type: ValueType,
	) -> Vec<u8> {
		if from.value == Value::None {
			return value_type.prefix_beg(ns, db, ix_what, ix_name);
		}
		let fd = Array::from(from.value.to_owned());
		if from.inclusive {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &fd)
		} else {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &fd)
		}
	}

	fn compute_end(
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		to: &RangeValue,
		value_type: ValueType,
	) -> Vec<u8> {
		if to.value == Value::None {
			return value_type.prefix_end(ns, db, ix_what, ix_name);
		}
		let fd = Array::from(to.value.to_owned());
		if to.inclusive {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &fd)
		} else {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &fd)
		}
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		let res = tx.scan(min..max, limit, None).await?;
		if let Some((key, _)) = res.last() {
			self.r.beg.clone_from(key);
			self.r.beg.push(0x00);
		}
		let mut records = B::with_capacity(res.len());
		res.into_iter()
			.filter(|(k, _)| self.r.matches(k))
			.for_each(|(_, v)| records.add((Arc::new(v.into()), self.irf.into(), None)));
		Ok(records)
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
		a: &Value,
	) -> Self {
		// We create a VecDeque to hold the prefix keys (begin and end) for each value in the array.
		let mut values: VecDeque<(Vec<u8>, Vec<u8>)> = if let Value::Array(a) = a {
			a.0.iter()
				.map(|v| {
					let a = Array::from(v.clone());
					let beg = Index::prefix_ids_beg(ns, db, &ix.what, &ix.name, &a);
					let end = Index::prefix_ids_end(ns, db, &ix.what, &ix.name, &a);
					(beg, end)
				})
				.collect()
		} else {
			VecDeque::with_capacity(0)
		};
		let current = values.pop_front();
		Self {
			irf,
			values,
			current,
		}
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		ctx: &Context,
		tx: &Transaction,
		limit: u32,
	) -> Result<B, Error> {
		while let Some(r) = &mut self.current {
			if ctx.is_done() {
				break;
			}
			let records: B =
				IndexEqualThingIterator::next_scan(tx, self.irf, &mut r.0, &r.1, limit).await?;
			if !records.is_empty() {
				return Ok(records);
			}
			self.current = self.values.pop_front();
		}
		Ok(B::empty())
	}
}

struct JoinThingIterator {
	ns: String,
	db: String,
	ix: Arc<DefineIndexStatement>,
	remote_iterators: VecDeque<ThingIterator>,
	current_remote: Option<ThingIterator>,
	current_remote_batch: VecDeque<CollectorRecord>,
	current_local: Option<ThingIterator>,
	distinct: Trie<Key, bool>,
}

impl JoinThingIterator {
	pub(super) fn new(
		opt: &Options,
		ix: Arc<DefineIndexStatement>,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Result<Self, Error> {
		Ok(Self {
			ns: opt.ns()?.to_string(),
			db: opt.db()?.to_string(),
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
		while !ctx.is_done() {
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
		F: Fn(&str, &str, &DefineIndexStatement, Value) -> ThingIterator,
	{
		while !ctx.is_done() {
			while let Some((thing, _, _)) = self.current_remote_batch.pop_front() {
				let k: Key = thing.as_ref().into();
				let value = Value::from(thing.as_ref().clone());
				if self.distinct.insert(k, true).is_none() {
					self.current_local = Some(new_iter(&self.ns, &self.db, &self.ix, value));
					return Ok(true);
				}
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
		F: Fn(&str, &str, &DefineIndexStatement, Value) -> ThingIterator + Copy,
	{
		while !ctx.is_done() {
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
}

pub(crate) struct IndexJoinThingIterator(IteratorRef, JoinThingIterator);

impl IndexJoinThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		opt: &Options,
		ix: Arc<DefineIndexStatement>,
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
			let it = IndexEqualThingIterator::new(self.0, ns, db, ix, &value);
			ThingIterator::IndexEqual(it)
		};
		self.1.next_batch(ctx, tx, limit, new_iter).await
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
		v: &Value,
	) -> Self {
		let a = Array::from(v.to_owned());
		let key = Index::new(ns, db, &ix.what, &ix.name, &a, None).into();
		Self {
			irf,
			key: Some(key),
		}
	}

	async fn next_batch<B: IteratorBatch>(&mut self, tx: &Transaction) -> Result<B, Error> {
		if let Some(key) = self.key.take() {
			if let Some(val) = tx.get(key, None).await? {
				let rid: Thing = val.into();
				let record = (rid.into(), self.irf.into(), None);
				return Ok(B::from_one(record));
			}
		}
		Ok(B::empty())
	}
}

pub(crate) struct UniqueRangeThingIterator {
	irf: IteratorRef,
	r: RangeScan,
	done: bool,
}

impl UniqueRangeThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		range: &IteratorRange<'_>,
	) -> Self {
		let beg = Self::compute_beg(ns, db, ix_what, ix_name, &range.from, range.value_type);
		let end = Self::compute_end(ns, db, ix_what, ix_name, &range.to, range.value_type);
		Self {
			irf,
			r: RangeScan::new(beg, range.from.inclusive, end, range.to.inclusive),
			done: false,
		}
	}

	pub(super) fn full_range(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix: &DefineIndexStatement,
	) -> Self {
		let value = RangeValue {
			value: Value::None,
			inclusive: true,
		};
		let range = IteratorRange {
			value_type: ValueType::None,
			from: Cow::Borrowed(&value),
			to: Cow::Borrowed(&value),
		};
		Self::new(irf, ns, db, &ix.what, &ix.name, &range)
	}

	fn compute_beg(
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		from: &RangeValue,
		value_type: ValueType,
	) -> Vec<u8> {
		if from.value == Value::None {
			return value_type.prefix_beg(ns, db, ix_what, ix_name);
		}
		Index::new(ns, db, ix_what, ix_name, &Array::from(from.value.to_owned()), None)
			.encode()
			.unwrap()
	}

	fn compute_end(
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		to: &RangeValue,
		value_type: ValueType,
	) -> Vec<u8> {
		if to.value == Value::None {
			return value_type.prefix_end(ns, db, ix_what, ix_name);
		}
		Index::new(ns, db, ix_what, ix_name, &Array::from(to.value.to_owned()), None)
			.encode()
			.unwrap()
	}

	async fn next_batch<B: IteratorBatch>(
		&mut self,
		tx: &Transaction,
		mut limit: u32,
	) -> Result<B, Error> {
		if self.done {
			return Ok(B::empty());
		}
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		limit += 1;
		let res = tx.scan(min..max, limit, None).await?;
		let mut records = B::with_capacity(res.len());
		for (k, v) in res {
			limit -= 1;
			if limit == 0 {
				self.r.beg = k;
				return Ok(records);
			}
			if self.r.matches(&k) {
				let rid: Thing = v.into();
				records.add((rid.into(), self.irf.into(), None));
			}
		}
		let end = self.r.end.clone();
		if self.r.matches(&end) {
			if let Some(v) = tx.get(end, None).await? {
				let rid: Thing = v.into();
				records.add((rid.into(), self.irf.into(), None));
			}
		}
		self.done = true;
		Ok(records)
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
		a: &Value,
	) -> Result<Self, Error> {
		// We create a VecDeque to hold the key for each value in the array.
		let keys: VecDeque<Key> = if let Value::Array(a) = a {
			a.0.iter()
				.map(|v| -> Result<Key, Error> {
					let a = Array::from(v.clone());
					let key = Index::new(opt.ns()?, opt.db()?, &ix.what, &ix.name, &a, None).into();
					Ok(key)
				})
				.collect::<Result<VecDeque<Key>, Error>>()?
		} else {
			VecDeque::with_capacity(0)
		};
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
		while let Some(key) = self.keys.pop_front() {
			if ctx.is_done() {
				break;
			}
			if let Some(val) = tx.get(key, None).await? {
				let rid: Thing = val.into();
				results.add((rid.into(), self.irf.into(), None));
				if results.len() >= limit {
					break;
				}
			}
		}
		Ok(results)
	}
}

pub(crate) struct UniqueJoinThingIterator(IteratorRef, JoinThingIterator);

impl UniqueJoinThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		opt: &Options,
		ix: Arc<DefineIndexStatement>,
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
			let it = UniqueEqualThingIterator::new(self.0, ns, db, ix, &value);
			ThingIterator::UniqueEqual(it)
		};
		self.1.next_batch(ctx, tx, limit, new_iter).await
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
			while limit > records.len() && !ctx.is_done() {
				if let Some((thg, doc_id)) = hits.next(tx).await? {
					let ir = IteratorRecord {
						irf: self.irf,
						doc_id: Some(doc_id),
						dist: None,
					};
					records.add((thg.into(), ir, None));
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
		while limit > records.len() && !ctx.is_done() {
			if let Some((thing, dist, val)) = self.res.pop_front() {
				let ir = IteratorRecord {
					irf: self.irf,
					doc_id: None,
					dist: Some(dist),
				};
				records.add((thing, ir, val));
			} else {
				break;
			}
		}
		Ok(records)
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
}
