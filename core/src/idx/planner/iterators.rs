use crate::dbs::Options;
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::{FtIndex, HitsIterator};
use crate::idx::planner::plan::RangeValue;
use crate::key::index::Index;
use crate::kvs;
use crate::kvs::{Key, Limit, ScanPage};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Ident, Thing, Value};
use radix_trie::Trie;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

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
	Knn(MtreeKnnIterator),
	Things(HnswKnnIterator),
}

impl ThingIterator {
	pub(crate) async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		size: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		match self {
			Self::IndexEqual(i) => i.next_batch(tx, size, collector).await,
			Self::UniqueEqual(i) => i.next_batch(tx, collector).await,
			Self::IndexRange(i) => i.next_batch(tx, size, collector).await,
			Self::UniqueRange(i) => i.next_batch(tx, size, collector).await,
			Self::IndexUnion(i) => i.next_batch(tx, size, collector).await,
			Self::UniqueUnion(i) => i.next_batch(tx, size, collector).await,
			Self::Matches(i) => i.next_batch(tx, size, collector).await,
			Self::Knn(i) => i.next_batch(tx, size, collector).await,
			Self::IndexJoin(i) => Box::pin(i.next_batch(tx, size, collector)).await,
			Self::UniqueJoin(i) => Box::pin(i.next_batch(tx, size, collector)).await,
			Self::Things(i) => i.next_batch(tx, size, collector).await,
		}
	}
}

pub(crate) type CollectorRecord = (Thing, IteratorRecord);

pub(crate) trait ThingCollector {
	async fn add(
		&mut self,
		tx: &mut kvs::Transaction,
		record: CollectorRecord,
	) -> Result<bool, Error>;
}

impl ThingCollector for VecDeque<(Thing, IteratorRecord)> {
	async fn add(
		&mut self,
		_tx: &mut kvs::Transaction,
		record: CollectorRecord,
	) -> Result<bool, Error> {
		self.push_back((record.0, record.1));
		Ok(true)
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
		ix_what: &Ident,
		ix_name: &Ident,
		v: &Value,
	) -> Self {
		let a = Array::from(v.clone());
		let beg = Index::prefix_ids_beg(ns, db, ix_what, ix_name, &a);
		let end = Index::prefix_ids_end(ns, db, ix_what, ix_name, &a);
		Self {
			irf,
			beg,
			end,
		}
	}

	async fn next_scan<T: ThingCollector>(
		tx: &mut kvs::Transaction,
		irf: IteratorRef,
		beg: &mut Vec<u8>,
		end: &[u8],
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let min = beg.clone();
		let max = end.to_owned();
		let res = tx
			.scan_paged(
				ScanPage {
					range: min..max,
					limit: Limit::Limited(limit),
				},
				limit,
			)
			.await?;
		let res = res.values;
		if let Some((key, _)) = res.last() {
			let mut key = key.clone();
			key.push(0x00);
			*beg = key;
		}
		let mut count = 0;
		for (_, val) in res {
			if !collector.add(tx, (val.into(), irf.into())).await? {
				break;
			}
			count += 1;
		}
		Ok(count)
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		Self::next_scan(tx, self.irf, &mut self.beg, &self.end, limit, collector).await
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
		from: &RangeValue,
		to: &RangeValue,
	) -> Self {
		let beg = Self::compute_beg(ns, db, ix_what, ix_name, from);
		let end = Self::compute_end(ns, db, ix_what, ix_name, to);
		Self {
			irf,
			r: RangeScan::new(beg, from.inclusive, end, to.inclusive),
		}
	}

	fn compute_beg(
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		from: &RangeValue,
	) -> Vec<u8> {
		if from.value == Value::None {
			return Index::prefix_beg(ns, db, ix_what, ix_name);
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
	) -> Vec<u8> {
		if to.value == Value::None {
			return Index::prefix_end(ns, db, ix_what, ix_name);
		}
		let fd = Array::from(to.value.to_owned());
		if to.inclusive {
			Index::prefix_ids_end(ns, db, ix_what, ix_name, &fd)
		} else {
			Index::prefix_ids_beg(ns, db, ix_what, ix_name, &fd)
		}
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		let res = tx
			.scan_paged(
				ScanPage {
					range: min..max,
					limit: Limit::Limited(limit),
				},
				limit,
			)
			.await?;
		let res = res.values;
		if let Some((key, _)) = res.last() {
			self.r.beg.clone_from(key);
			self.r.beg.push(0x00);
		}
		let mut count = 0;
		for (k, v) in res {
			if self.r.matches(&k) {
				count += 1;
				if !collector.add(tx, (v.into(), self.irf.into())).await? {
					break;
				}
			}
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
		ix_what: &Ident,
		ix_name: &Ident,
		a: &Array,
	) -> Self {
		// We create a VecDeque to hold the prefix keys (begin and end) for each value in the array.
		let mut values: VecDeque<(Vec<u8>, Vec<u8>)> =
			a.0.iter()
				.map(|v| {
					let a = Array::from(v.clone());
					let beg = Index::prefix_ids_beg(ns, db, ix_what, ix_name, &a);
					let end = Index::prefix_ids_end(ns, db, ix_what, ix_name, &a);
					(beg, end)
				})
				.collect();
		let current = values.pop_front();
		Self {
			irf,
			values,
			current,
		}
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		while let Some(r) = &mut self.current {
			let count =
				IndexEqualThingIterator::next_scan(tx, self.irf, &mut r.0, &r.1, limit, collector)
					.await?;
			if count != 0 {
				return Ok(count);
			}
			self.current = self.values.pop_front();
		}
		Ok(0)
	}
}

struct JoinThingIterator {
	ns: String,
	db: String,
	ix_what: Ident,
	ix_name: Ident,
	remote_iterators: VecDeque<ThingIterator>,
	current_remote: Option<ThingIterator>,
	current_remote_batch: VecDeque<CollectorRecord>,
	current_local: Option<ThingIterator>,
	distinct: Trie<Key, bool>,
}

impl JoinThingIterator {
	pub(super) fn new(
		opt: &Options,
		ix: &DefineIndexStatement,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Self {
		Self {
			ns: opt.ns().to_string(),
			db: opt.db().to_string(),
			ix_what: ix.what.clone(),
			ix_name: ix.name.clone(),
			current_remote: None,
			current_remote_batch: VecDeque::with_capacity(0),
			remote_iterators,
			current_local: None,
			distinct: Default::default(),
		}
	}
}

impl JoinThingIterator {
	async fn next_current_remote_batch(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
	) -> Result<bool, Error> {
		loop {
			if let Some(it) = &mut self.current_remote {
				self.current_remote_batch.clear();
				if it.next_batch(tx, limit, &mut self.current_remote_batch).await? > 0 {
					return Ok(true);
				}
			}
			self.current_remote = self.remote_iterators.pop_front();
			if self.current_remote.is_none() {
				return Ok(false);
			}
		}
	}

	async fn next_current_local<F>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		new_iter: F,
	) -> Result<bool, Error>
	where
		F: Fn(&str, &str, &Ident, &Ident, Value) -> ThingIterator,
	{
		loop {
			while let Some((thing, _)) = self.current_remote_batch.pop_front() {
				let k: Key = (&thing).into();
				let value = Value::from(thing);
				if self.distinct.insert(k, true).is_none() {
					self.current_local =
						Some(new_iter(&self.ns, &self.db, &self.ix_what, &self.ix_name, value));
					return Ok(true);
				}
			}
			if !self.next_current_remote_batch(tx, limit).await? {
				break;
			}
		}
		Ok(false)
	}

	async fn next_batch<T: ThingCollector, F>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
		new_iter: F,
	) -> Result<usize, Error>
	where
		F: Fn(&str, &str, &Ident, &Ident, Value) -> ThingIterator + Copy,
	{
		loop {
			if let Some(current_local) = &mut self.current_local {
				let n = current_local.next_batch(tx, limit, collector).await?;
				if n > 0 {
					return Ok(n);
				}
			}
			if !self.next_current_local(tx, limit, new_iter).await? {
				return Ok(0);
			}
		}
	}
}

pub(crate) struct IndexJoinThingIterator(IteratorRef, JoinThingIterator);

impl IndexJoinThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Self {
		Self(irf, JoinThingIterator::new(opt, ix, remote_iterators))
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let new_iter = |ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident, value: Value| {
			let it = IndexEqualThingIterator::new(self.0, ns, db, ix_what, ix_name, &value);
			ThingIterator::IndexEqual(it)
		};
		self.1.next_batch(tx, limit, collector, new_iter).await
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
		ix_what: &Ident,
		ix_name: &Ident,
		v: &Value,
	) -> Self {
		let a = Array::from(v.to_owned());
		let key = Index::new(ns, db, ix_what, ix_name, &a, None).into();
		Self {
			irf,
			key: Some(key),
		}
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		collector: &mut T,
	) -> Result<usize, Error> {
		if let Some(key) = self.key.take() {
			if let Some(val) = tx.get(key).await? {
				collector.add(tx, (val.into(), self.irf.into())).await?;
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
	pub(super) fn new(
		irf: IteratorRef,
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		from: &RangeValue,
		to: &RangeValue,
	) -> Self {
		let beg = Self::compute_beg(ns, db, ix_what, ix_name, from);
		let end = Self::compute_end(ns, db, ix_what, ix_name, to);
		Self {
			irf,
			r: RangeScan::new(beg, from.inclusive, end, to.inclusive),
			done: false,
		}
	}

	fn compute_beg(
		ns: &str,
		db: &str,
		ix_what: &Ident,
		ix_name: &Ident,
		from: &RangeValue,
	) -> Vec<u8> {
		if from.value == Value::None {
			return Index::prefix_beg(ns, db, ix_what, ix_name);
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
	) -> Vec<u8> {
		if to.value == Value::None {
			return Index::prefix_end(ns, db, ix_what, ix_name);
		}
		Index::new(ns, db, ix_what, ix_name, &Array::from(to.value.to_owned()), None)
			.encode()
			.unwrap()
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		mut limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		if self.done {
			return Ok(0);
		}
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		limit += 1;
		let res = tx
			.scan_paged(
				ScanPage {
					range: min..max,
					limit: Limit::Limited(limit),
				},
				limit,
			)
			.await?;
		let mut count = 0;
		for (k, v) in res.values {
			limit -= 1;
			if limit == 0 {
				self.r.beg = k;
				return Ok(count);
			}
			if self.r.matches(&k) {
				count += 1;
				if !collector.add(tx, (v.into(), self.irf.into())).await? {
					break;
				}
			}
		}
		let end = self.r.end.clone();
		if self.r.matches(&end) {
			if let Some(v) = tx.get(end).await? {
				count += 1;
				collector.add(tx, (v.into(), self.irf.into())).await?;
			}
		}
		self.done = true;
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
		a: &Array,
	) -> Self {
		// We create a VecDeque to hold the key for each value in the array.
		let keys: VecDeque<Key> =
			a.0.iter()
				.map(|v| {
					let a = Array::from(v.clone());
					let key = Index::new(opt.ns(), opt.db(), &ix.what, &ix.name, &a, None).into();
					key
				})
				.collect();
		Self {
			irf,
			keys,
		}
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let mut count = 0;
		while let Some(key) = self.keys.pop_front() {
			if let Some(val) = tx.get(key).await? {
				count += 1;
				if !collector.add(tx, (val.into(), self.irf.into())).await? {
					break;
				}
				if count >= limit {
					break;
				}
			}
		}
		Ok(count as usize)
	}
}

pub(crate) struct UniqueJoinThingIterator(IteratorRef, JoinThingIterator);

impl UniqueJoinThingIterator {
	pub(super) fn new(
		irf: IteratorRef,
		opt: &Options,
		ix: &DefineIndexStatement,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Self {
		Self(irf, JoinThingIterator::new(opt, ix, remote_iterators))
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let new_iter = |ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident, value: Value| {
			let it = UniqueEqualThingIterator::new(self.0, ns, db, ix_what, ix_name, &value);
			ThingIterator::UniqueEqual(it)
		};
		self.1.next_batch(tx, limit, collector, new_iter).await
	}
}

pub(crate) struct MatchesThingIterator {
	irf: IteratorRef,
	hits: Option<HitsIterator>,
}

impl MatchesThingIterator {
	pub(super) async fn new(
		irf: IteratorRef,
		fti: &FtIndex,
		terms_docs: TermsDocs,
	) -> Result<Self, Error> {
		let hits = fti.new_hits_iterator(terms_docs)?;
		Ok(Self {
			irf,
			hits,
		})
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let mut count = 0;
		if let Some(hits) = &mut self.hits {
			while limit > count {
				if let Some((thg, doc_id)) = hits.next(tx).await? {
					count += 1;
					if !collector
						.add(
							tx,
							(
								thg,
								IteratorRecord {
									irf: self.irf,
									doc_id: Some(doc_id),
									dist: None,
								},
							),
						)
						.await?
					{
						break;
					}
				} else {
					break;
				}
			}
		}
		Ok(count as usize)
	}
}

pub(crate) struct MtreeKnnIterator {
	irf: IteratorRef,
	doc_ids: Arc<RwLock<DocIds>>,
	res: VecDeque<(DocId, f64)>,
}

impl MtreeKnnIterator {
	pub(super) fn new(
		irf: IteratorRef,
		doc_ids: Arc<RwLock<DocIds>>,
		res: VecDeque<(DocId, f64)>,
	) -> Self {
		Self {
			irf,
			doc_ids,
			res,
		}
	}
	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let mut count = 0;
		while limit > count {
			if let Some((doc_id, dist)) = self.res.pop_front() {
				if let Some(doc_key) = self.doc_ids.read().await.get_doc_key(tx, doc_id).await? {
					count += 1;
					if !collector
						.add(
							tx,
							(
								doc_key.into(),
								IteratorRecord {
									irf: self.irf,
									doc_id: Some(doc_id),
									dist: Some(dist),
								},
							),
						)
						.await?
					{
						break;
					}
				}
			} else {
				break;
			}
		}
		Ok(count as usize)
	}
}

pub(crate) struct HnswKnnIterator {
	irf: IteratorRef,
	res: VecDeque<(Thing, f64)>,
}

impl HnswKnnIterator {
	pub(super) fn new(irf: IteratorRef, res: VecDeque<(Thing, f64)>) -> Self {
		Self {
			irf,
			res,
		}
	}
	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &mut kvs::Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let mut count = 0;
		while limit > count {
			if let Some((thg, dist)) = self.res.pop_front() {
				count += 1;
				if !collector
					.add(
						tx,
						(
							thg,
							IteratorRecord {
								irf: self.irf,
								doc_id: None,
								dist: Some(dist),
							},
						),
					)
					.await?
				{
					break;
				}
			} else {
				break;
			}
		}
		Ok(count as usize)
	}
}
