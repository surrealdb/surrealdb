use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::ft::termdocs::TermsDocs;
use crate::idx::ft::{FtIndex, HitsIterator};
use crate::idx::planner::plan::RangeValue;
use crate::key::index::Index;
use crate::kvs::{Key, Limit, ScanPage};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Ident, Thing, Value};
use radix_trie::Trie;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

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
	Knn(DocIdsIterator),
}

impl ThingIterator {
	pub(crate) async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &Transaction,
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
		}
	}
}

pub(crate) trait ThingCollector {
	fn add(&mut self, thing: Thing, doc_id: Option<DocId>);
}

impl ThingCollector for Vec<(Thing, Option<DocId>)> {
	fn add(&mut self, thing: Thing, doc_id: Option<DocId>) {
		self.push((thing, doc_id));
	}
}

impl ThingCollector for VecDeque<(Thing, Option<DocId>)> {
	fn add(&mut self, thing: Thing, doc_id: Option<DocId>) {
		self.push_back((thing, doc_id));
	}
}

pub(crate) struct IndexEqualThingIterator {
	beg: Vec<u8>,
	end: Vec<u8>,
}

impl IndexEqualThingIterator {
	pub(super) fn new(ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident, v: &Value) -> Self {
		let a = Array::from(v.clone());
		let beg = Index::prefix_ids_beg(ns, db, ix_what, ix_name, &a);
		let end = Index::prefix_ids_end(ns, db, ix_what, ix_name, &a);
		Self {
			beg,
			end,
		}
	}

	async fn next_scan<T: ThingCollector>(
		txn: &Transaction,
		beg: &mut Vec<u8>,
		end: &[u8],
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let min = beg.clone();
		let max = end.to_owned();
		let res = txn
			.lock()
			.await
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
		let count = res.len();
		res.into_iter().for_each(|(_, val)| collector.add(val.into(), None));
		Ok(count)
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		txn: &Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		Self::next_scan(txn, &mut self.beg, &self.end, limit, collector).await
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
	r: RangeScan,
}

impl IndexRangeThingIterator {
	pub(super) fn new(
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
		txn: &Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		let res = txn
			.lock()
			.await
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
				collector.add(v.into(), None);
				count += 1;
			}
		}
		Ok(count)
	}
}

pub(crate) struct IndexUnionThingIterator {
	values: VecDeque<(Vec<u8>, Vec<u8>)>,
	current: Option<(Vec<u8>, Vec<u8>)>,
}

impl IndexUnionThingIterator {
	pub(super) fn new(ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident, a: &Array) -> Self {
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
			values,
			current,
		}
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		txn: &Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		while let Some(r) = &mut self.current {
			let count =
				IndexEqualThingIterator::next_scan(txn, &mut r.0, &r.1, limit, collector).await?;
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
	current_remote_batch: VecDeque<(Thing, Option<DocId>)>,
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
		tx: &Transaction,
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
		tx: &Transaction,
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
		tx: &Transaction,
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

pub(crate) struct IndexJoinThingIterator(JoinThingIterator);

impl IndexJoinThingIterator {
	pub(super) fn new(
		opt: &Options,
		ix: &DefineIndexStatement,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Self {
		Self(JoinThingIterator::new(opt, ix, remote_iterators))
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let new_iter = |ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident, value: Value| {
			let it = IndexEqualThingIterator::new(ns, db, ix_what, ix_name, &value);
			ThingIterator::IndexEqual(it)
		};
		self.0.next_batch(tx, limit, collector, new_iter).await
	}
}

pub(crate) struct UniqueEqualThingIterator {
	key: Option<Key>,
}

impl UniqueEqualThingIterator {
	pub(super) fn new(ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident, v: &Value) -> Self {
		let a = Array::from(v.to_owned());
		let key = Index::new(ns, db, ix_what, ix_name, &a, None).into();
		Self {
			key: Some(key),
		}
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		txn: &Transaction,
		collector: &mut T,
	) -> Result<usize, Error> {
		let mut count = 0;
		if let Some(key) = self.key.take() {
			if let Some(val) = txn.lock().await.get(key).await? {
				collector.add(val.into(), None);
				count += 1;
			}
		}
		Ok(count)
	}
}

pub(crate) struct UniqueRangeThingIterator {
	r: RangeScan,
	done: bool,
}

impl UniqueRangeThingIterator {
	pub(super) fn new(
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
		txn: &Transaction,
		mut limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		if self.done {
			return Ok(0);
		}
		let min = self.r.beg.clone();
		let max = self.r.end.clone();
		limit += 1;
		let mut tx = txn.lock().await;
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
				collector.add(v.into(), None);
				count += 1;
			}
		}
		let end = self.r.end.clone();
		if self.r.matches(&end) {
			if let Some(v) = tx.get(end).await? {
				collector.add(v.into(), None);
				count += 1;
			}
		}
		self.done = true;
		Ok(count)
	}
}

pub(crate) struct UniqueUnionThingIterator {
	keys: VecDeque<Key>,
}

impl UniqueUnionThingIterator {
	pub(super) fn new(opt: &Options, ix: &DefineIndexStatement, a: &Array) -> Self {
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
			keys,
		}
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		txn: &Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let mut run = txn.lock().await;
		let mut count = 0;
		while let Some(key) = self.keys.pop_front() {
			if let Some(val) = run.get(key).await? {
				collector.add(val.into(), None);
				count += 1;
				if count >= limit {
					break;
				}
			}
		}
		Ok(count as usize)
	}
}

pub(crate) struct UniqueJoinThingIterator(JoinThingIterator);

impl UniqueJoinThingIterator {
	pub(super) fn new(
		opt: &Options,
		ix: &DefineIndexStatement,
		remote_iterators: VecDeque<ThingIterator>,
	) -> Self {
		Self(JoinThingIterator::new(opt, ix, remote_iterators))
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		tx: &Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let new_iter = |ns: &str, db: &str, ix_what: &Ident, ix_name: &Ident, value: Value| {
			let it = UniqueEqualThingIterator::new(ns, db, ix_what, ix_name, &value);
			ThingIterator::UniqueEqual(it)
		};
		self.0.next_batch(tx, limit, collector, new_iter).await
	}
}

pub(crate) struct MatchesThingIterator {
	hits: Option<HitsIterator>,
}

impl MatchesThingIterator {
	pub(super) async fn new(fti: &FtIndex, terms_docs: TermsDocs) -> Result<Self, Error> {
		let hits = fti.new_hits_iterator(terms_docs)?;
		Ok(Self {
			hits,
		})
	}

	async fn next_batch<T: ThingCollector>(
		&mut self,
		txn: &Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let mut count = 0;
		if let Some(hits) = &mut self.hits {
			let mut run = txn.lock().await;
			while limit > count {
				if let Some((thg, doc_id)) = hits.next(&mut run).await? {
					collector.add(thg, Some(doc_id));
					count += 1;
				} else {
					break;
				}
			}
		}
		Ok(count as usize)
	}
}

pub(crate) struct DocIdsIterator {
	doc_ids: Arc<RwLock<DocIds>>,
	res: VecDeque<DocId>,
}

impl DocIdsIterator {
	pub(super) fn new(doc_ids: Arc<RwLock<DocIds>>, res: VecDeque<DocId>) -> Self {
		Self {
			doc_ids,
			res,
		}
	}
	async fn next_batch<T: ThingCollector>(
		&mut self,
		txn: &Transaction,
		limit: u32,
		collector: &mut T,
	) -> Result<usize, Error> {
		let mut tx = txn.lock().await;
		let mut count = 0;
		while limit > count {
			if let Some(doc_id) = self.res.pop_front() {
				if let Some(doc_key) =
					self.doc_ids.read().await.get_doc_key(&mut tx, doc_id).await?
				{
					collector.add(doc_key.into(), Some(doc_id));
					count += 1;
				}
			} else {
				break;
			}
		}
		Ok(count as usize)
	}
}
